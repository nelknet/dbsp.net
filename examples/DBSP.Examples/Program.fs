// DBSP.NET Examples - Naive vs Incremental (ZSet-based) tutorial
//
// Business scenario:
// - We have Customers and Orders. Each order belongs to a customer and has a dollar amount.
// - We want to compute, repeatedly over time, the count of active orders per customer.
//
// Two approaches:
// 1) Naive recalculation: On each step, scan all orders and recompute counts from scratch.
// 2) Incremental (DBSP-style): Represent changes as ZSet deltas keyed by CustomerId, then integrate deltas
//    over time to maintain current counts per customer efficiently.
//
// This tutorial prints what’s happening and times both approaches with Stopwatch.

open System
open System.Diagnostics
open System.Collections.Generic
open System.Threading.Tasks

open DBSP.Core.ZSet
open DBSP.Operators.TemporalOperators

// Domain models
type Customer = { Id: int; Name: string }
type Order = { Id: int; CustomerId: int; Amount: decimal }

// Simple data generator for demo purposes
module DataGen =
    let customers (count: int) =
        Array.init count (fun i -> { Id = i + 1; Name = $"Customer {i+1}" })

    let orders (customers: Customer[]) (count: int) =
        let rnd = Random(42)
        Array.init count (fun i ->
            let cust = customers[rnd.Next(customers.Length)]
            { Id = i + 1; CustomerId = cust.Id; Amount = decimal (10 + rnd.Next(990)) })

// Naive recomputation: scan all current orders each step and rebuild counts
module Naive =
    let recomputeOrderCountsByCustomer (orders: Order[]) =
        let counts = Dictionary<int,int>()
        for o in orders do
            if counts.ContainsKey o.CustomerId then counts[o.CustomerId] <- counts[o.CustomerId] + 1
            else counts[o.CustomerId] <- 1
        counts

// Incremental (DBSP-style using ZSet and IntegrateOperator)
// We treat each order as contributing +1 to its customer’s count. On updates that change the order’s customer,
// we emit a -1 for the old customer and a +1 for the new customer. Deletions emit -1. Insertions emit +1.
module Incremental =
    // Build a ZSet delta for a batch of changes to orders
    // changes: sequence of (oldOrder option, newOrder option)
    //  - Insert: (None, Some new)
    //  - Delete: (Some old, None)
    //  - Update: (Some old, Some new)
    let buildDelta (changes: seq<Order option * Order option>) : ZSet<int> =
        ZSet.buildWith (fun b ->
            for (oldO, newO) in changes do
                match oldO, newO with
                | None, Some n -> // insert
                    b.Add(n.CustomerId, 1)
                | Some o, None -> // delete
                    b.Add(o.CustomerId, -1)
                | Some o, Some n -> // update (possibly customer change)
                    if o.CustomerId <> n.CustomerId then
                        b.Add(o.CustomerId, -1)
                        b.Add(n.CustomerId, 1)
                | None, None -> () )

    // Use IntegrateOperator to accumulate deltas into a running count per customer
    let integrateDeltas (deltas: ZSet<int> seq) : ZSet<int> =
        let op = new IntegrateOperator<int>()
        deltas
        |> Seq.fold (fun acc delta ->
            // Feed delta to Integrate; Integrate returns the current accumulator
            op.EvalAsyncImpl(delta).Result) (ZSet.empty<int>)

// Utilities for pretty printing
let printTopCounts (title: string) (counts: seq<int * int>) (customers: Customer[]) (top: int) =
    printfn "%s" title
    let lookup = dict [ for c in customers -> c.Id, c.Name ]
    counts
    |> Seq.sortByDescending snd
    |> Seq.truncate top
    |> Seq.iter (fun (cid, cnt) ->
        let name = defaultArg (lookup.TryGetValue cid |> function true, v -> Some v | _ -> None) $"Customer {cid}"
        printfn "  %-12s  -> %d orders" name cnt)
    printfn ""

[<EntryPoint>]
let main argv =
    // Tutorial parameters (larger defaults to accentuate naive vs incremental)
    // Override via CLI: --customers N --initial N --steps N --changes N
    let parseArg (name: string) (def: int) =
        let idx = argv |> Array.tryFindIndex (fun a -> a.Equals(name, StringComparison.OrdinalIgnoreCase))
        match idx with
        | Some i when i + 1 < argv.Length ->
            match Int32.TryParse(argv[i+1]) with
            | true, v -> v
            | _ -> def
        | _ -> def
    // Defaults tuned to show seconds (naive) vs milliseconds (incremental)
    // You can override via CLI flags.
    let customerCount = parseArg "--customers" 100
    let initialOrderCount = parseArg "--initial" 5000000
    let steps = parseArg "--steps" 60
    let changesPerStep = parseArg "--changes" 100

    printfn "DBSP.NET Tutorial: Naive vs Incremental Order Counts\n"
    printfn "Scenario: %d customers, %d initial orders, %d steps, %d changes/step"
            customerCount initialOrderCount steps changesPerStep
    printfn ""

    // Generate base data
    let customers = DataGen.customers customerCount
    let initialOrders = DataGen.orders customers initialOrderCount
    // Insert-only demo flag (accentuates incremental advantage)
    let insertOnly = true
    // Mutable store of current orders for naive recomputation (fast append)
    let ordersList = ResizeArray<Order>(initialOrders.Length + steps * changesPerStep)
    ordersList.AddRange initialOrders
    // Mutable next id for new inserts
    let mutable nextOrderId = if initialOrders.Length = 0 then 1 else (initialOrders |> Array.maxBy (fun o -> o.Id)).Id + 1

    // Helper to create random changes (insert/update/delete)
    let rnd = Random(7)
    let randomInserts (n: int) : Order[] =
        Array.init n (fun _ ->
            let cust = customers[rnd.Next(customers.Length)]
            let order = { Id = nextOrderId; CustomerId = cust.Id; Amount = decimal (10 + rnd.Next(990)) }
            nextOrderId <- nextOrderId + 1
            ordersList.Add(order)
            order)

    // 1) Naive timing loop
    printfn "--- Naive recomputation ---"
    // Precompute baseline counts outside the timed region
    let mutable lastNaiveCounts : Dictionary<int,int> = Naive.recomputeOrderCountsByCustomer (ordersList.ToArray())
    let swNaive = Stopwatch.StartNew()
    for step in 1..steps do
        let _newOrders = randomInserts changesPerStep
        // Recompute from scratch each step
        lastNaiveCounts <- Naive.recomputeOrderCountsByCustomer (ordersList.ToArray())
        if step % max 1 (steps / 2) = 0 then
            let top = lastNaiveCounts |> Seq.map (fun kv -> kv.Key, kv.Value)
            printTopCounts (sprintf "Naive top counts after step %d:" step) top customers 5
    swNaive.Stop()
    printfn "Naive total time: %d ms\n" swNaive.ElapsedMilliseconds

    // 2) Incremental timing loop (DBSP-style ZSet + Integrate)
    // Start from empty and feed deltas that reflect changes from the original initialOrders state.
    printfn "--- Incremental (ZSet + Integrate) ---"
    // Build an initial delta that inserts all current orders’ customer contributions
    // Build baseline accumulator outside timing (one-time pass over initial orders)
    let baselineCounts =
        let d = Dictionary<int,int>()
        for o in ordersList do
            let v = if d.ContainsKey o.CustomerId then d[o.CustomerId] + 1 else 1
            d[o.CustomerId] <- v
        d
    let accBaseline =
        ZSet.buildWith (fun b ->
            for kv in baselineCounts do b.Add(kv.Key, kv.Value))
    let integrate = new IntegrateOperator<int>()
    let mutable acc = accBaseline

    // Now perform the same sequence of random steps, but incrementally
    // Note: to make timing comparable to the naive loop, we regenerate a fresh set of changes here.
    // In real usage you’d feed the same incoming change stream to both implementations.
    let swIncr = Stopwatch.StartNew()
    for step in 1..steps do
        // Insert-only delta for this step
        let ins = randomInserts changesPerStep
        let delta = ZSet.buildWith (fun b -> for o in ins do b.Add(o.CustomerId, 1))
        acc <- integrate.EvalAsyncImpl(delta).Result
        if step % max 1 (steps / 2) = 0 then
            let top = acc |> ZSet.toSeq |> Seq.map (fun (k,w) -> k, w)
            printTopCounts (sprintf "Incremental top counts after step %d:" step) top customers 5
    swIncr.Stop()
    printfn "Incremental total time: %d ms\n" swIncr.ElapsedMilliseconds

    // Final comparison
    let naiveTotal = swNaive.ElapsedMilliseconds
    let incrTotal = swIncr.ElapsedMilliseconds
    printfn "Summary:\n  Naive total:       %d ms\n  Incremental total: %d ms\n  Speedup (naive/incr): %.2fx"
            naiveTotal incrTotal (if incrTotal = 0L then Double.PositiveInfinity else (float naiveTotal) / (float incrTotal))

    // Show how to read a specific customer’s current count from the incremental state
    let demoCustomerId = 1
    let currentCount = acc.GetWeight(demoCustomerId)
    printfn "\nCustomer %d currently has %d active orders (incremental state)." demoCustomerId currentCount

    0
