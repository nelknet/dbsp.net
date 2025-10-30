module DBSP.Tutorials.Performance

open System
open System.Collections.Generic
open System.Diagnostics
open DBSP.Core
open DBSP.Core.ZSet
open DBSP.Operators.TemporalOperators
open DBSP.Tutorials.Common

type Order = { Id: int; Customer: int }

let private buildDelta (changes: seq<Order option * Order option>) =
    let delta = Dictionary<int, int>()
    let accumulate key weight =
        if weight <> 0 then
            let current = if delta.ContainsKey key then delta[key] else 0
            let updated = current + weight
            if updated = 0 then delta.Remove key |> ignore else delta[key] <- updated
    for (beforeOpt, afterOpt) in changes do
        match beforeOpt, afterOpt with
        | None, Some afterOrder ->
            accumulate afterOrder.Customer 1
        | Some beforeOrder, None ->
            accumulate beforeOrder.Customer -1
        | Some beforeOrder, Some afterOrder ->
            if beforeOrder.Customer <> afterOrder.Customer then
                accumulate beforeOrder.Customer -1
                accumulate afterOrder.Customer 1
        | None, None -> ()
    let builder = ZSetDelta.Create<int>()
    delta
    |> Seq.iter (fun kvp -> builder.AddWeight(kvp.Key, kvp.Value) |> ignore)
    builder.ToZSet()

let private chooseExisting (rnd: Random) (store: Dictionary<int, int>) =
    if store.Count = 0 then None
    else
        let keys = Array.ofSeq store.Keys
        let id = keys[rnd.Next(keys.Length)]
        Some { Id = id; Customer = store[id] }

let run iterations changesPerStep =
    printHeader "Performance Harness"
    let rnd = Random(1234)
    let customerCount = 100
    let initialOrders = 50_000

    let orders = Dictionary<int, int>(initialOrders + iterations * changesPerStep)
    let mutable nextOrderId = 1

    // Seed initial data set
    for _ in 1 .. initialOrders do
        let order = { Id = nextOrderId; Customer = rnd.Next(customerCount) }
        orders[order.Id] <- order.Customer
        nextOrderId <- nextOrderId + 1

    // Prepare incremental baseline
    let integrate = new IntegrateOperator<int>()
    let baseline =
        let builder = ZSetDelta.Create<int>()
        orders.Values
        |> Seq.countBy id
        |> Seq.iter (fun (customer, count) -> builder.AddWeight(customer, count) |> ignore)
        builder.ToZSet()
    integrate.EvalAsyncImpl(baseline) |> runTask |> ignore

    let swNaive = Stopwatch()
    let swIncr = Stopwatch()

    let verifyState (expected: Map<int,int>) (actual: ZSet<int>) =
        let actualMap =
            actual
            |> ZSet.fold (fun acc key weight ->
                if weight <> 0 then
                    acc |> Map.change key (fun existing -> Some(weight + defaultArg existing 0))
                else
                    acc) Map.empty
        if expected <> actualMap then
            let diff =
                Seq.append
                    (expected |> Map.toSeq |> Seq.map (fun (k,v) -> $"expected {k}->{v}"))
                    (actualMap |> Map.toSeq |> Seq.map (fun (k,v) -> $"actual {k}->{v}"))
                |> String.concat "; "
            failwithf "Incremental state mismatch: %s" diff

    for step in 1 .. iterations do
        let changes = ResizeArray<Order option * Order option>(changesPerStep)

        for _ in 1 .. changesPerStep do
            let action = rnd.NextDouble()
            if action < 0.55 then
                // Insert
                let order = { Id = nextOrderId; Customer = rnd.Next(customerCount) }
                orders[order.Id] <- order.Customer
                nextOrderId <- nextOrderId + 1
                changes.Add(None, Some order)
            elif action < 0.8 then
                // Update existing customer mapping
                match chooseExisting rnd orders with
                | None ->
                    // fall back to insert
                    let order = { Id = nextOrderId; Customer = rnd.Next(customerCount) }
                    orders[order.Id] <- order.Customer
                    nextOrderId <- nextOrderId + 1
                    changes.Add(None, Some order)
                | Some oldOrder ->
                    let updated = { oldOrder with Customer = rnd.Next(customerCount) }
                    orders[updated.Id] <- updated.Customer
                    changes.Add(Some oldOrder, Some updated)
            else
                // Delete
                match chooseExisting rnd orders with
                | None ->
                    ()
                | Some oldOrder ->
                    orders.Remove oldOrder.Id |> ignore
                    changes.Add(Some oldOrder, None)

        let delta = buildDelta changes

        swNaive.Start()
        let naiveCounts =
            orders.Values
            |> Seq.countBy id
            |> Map.ofSeq
        swNaive.Stop()

        swIncr.Start()
        let incrementalState = integrate.EvalAsyncImpl(delta) |> runTask
        swIncr.Stop()

        verifyState naiveCounts incrementalState

        if step % max 1 (iterations / 5) = 0 then
            printfn "step %d -> in-flight changes %d" step changes.Count

    printfn "\nNaive total       : %d ms" swNaive.ElapsedMilliseconds
    printfn "Incremental total : %d ms" swIncr.ElapsedMilliseconds
    if swIncr.ElapsedMilliseconds > 0L then
        let speedup = float swNaive.ElapsedMilliseconds / float swIncr.ElapsedMilliseconds
        printfn "Speed-up          : %.2fx" speedup
