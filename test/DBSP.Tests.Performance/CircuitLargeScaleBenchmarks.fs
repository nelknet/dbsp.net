namespace DBSP.Tests.Performance

open System
open BenchmarkDotNet.Attributes
open BenchmarkDotNet.Jobs
open DBSP.Core.ZSet
open DBSP.Core.IndexedZSet
open DBSP.Operators
open DBSP.Operators.Interfaces
open DBSP.Operators.FusedOperators
open DBSP.Operators.LinearOperators
open DBSP.Operators.AggregateOperators
open DBSP.Operators.JoinOperators

// Synthetic order and dimension types (top-level)
type CircuitOrder = {
    OrderId: int
    CustomerId: int
    ProductId: int
    Quantity: int
    PriceCents: int
    Timestamp: int64
}

[<MemoryDiagnoser>]
type CircuitLargeScaleBenchmarks() =

    member this.DataSizes =
        let quick = System.String.Equals(System.Environment.GetEnvironmentVariable("DBSP_QUICK_BENCH"), "1", System.StringComparison.OrdinalIgnoreCase)
        if quick then [| 100000; 300000 |] else [| 100000; 1000000 |]
    [<ParamsSource("DataSizes")>]
    member val DataSize = 0 with get, set

    member this.ChangeCounts =
        let quick = System.String.Equals(System.Environment.GetEnvironmentVariable("DBSP_QUICK_BENCH"), "1", System.StringComparison.OrdinalIgnoreCase)
        if quick then [| 1; 100 |] else [| 1; 10; 100 |]
    [<ParamsSource("ChangeCounts")>]
    member val ChangeCount = 0 with get, set

    // Base data and dimension
    member val private baseLeft : CircuitOrder array = Array.empty with get, set
    member val private baseRight: (int * string) array = Array.empty with get, set
    member val private changes  : CircuitOrder array = Array.empty with get, set

    // Operators composing the pipeline
    // Stage 1: Filter recent AND map -> (productId, (customerId, amount))
    member val private filterMap: IUnaryOperator<ZSet<CircuitOrder>, ZSet<int * (int * int)>> =
        FilterMapOperator<CircuitOrder, int * (int * int)>(
            (fun o -> o.Timestamp >= (DateTimeOffset.UtcNow.ToUnixTimeSeconds() - int64 (60*60*24*30))),
            (fun o -> let amount = o.Quantity * o.PriceCents in (o.ProductId, (o.CustomerId, amount)))
        ) :> _ with get, set

    // Stage 2: Join with product class dimension (productId -> class)
    member val private joinOp = InnerJoinOperator<int, (int * int), string>() :> IBinaryOperator<ZSet<int * (int * int)>, ZSet<int * string>, IndexedZSet<int, (int * int) * string>> with get, set

    // Stage 3: Map join result to ((customerId, class) -> amount)
    member val private projectToAgg: IUnaryOperator<ZSet<int * ((int * int) * string)>, ZSet<(int * string) * int>> =
        MapOperator<ZSet<int * ((int * int) * string)>, ZSet<(int * string) * int>>(
            fun z ->
                ZSet.buildWith (fun builder ->
                    for ((pid, ((cid, amount), cls)), weight) in ZSet.toSeq z do
                        if weight <> 0 then builder.Add(((cid, cls), amount), weight)
                )
        ) :> _ with get, set

    // Stage 4: Aggregate sum by (customerId, class)
    member val private sumAgg: IUnaryOperator<ZSet<(int * string) * int>, ZSet<(int * string) * int>> =
        IntSumOperator<(int * string)>() :> _ with get, set

    // Cached right dimension as ZSet of (productId -> class)
    member val private rightZ: ZSet<int * string> = ZSet.empty with get, set

    // Fused JoinProject variant for join+projection
    member val private fusedJoinProject : DBSP.Operators.Interfaces.IBinaryOperator<ZSet<int * (int * int)>, ZSet<int * string>, ZSet<(int * string) * int>> =
        (DBSP.Operators.FusedOperators.JoinProjectOperator<int, (int * (int * int)), (int * string), (int * string) * int>(
            (fun (pid, _) -> pid),
            (fun (pid, _) -> pid),
            (fun ((_, (cid, amount))) (_, cls) -> ((cid, cls), amount))
        ) :> _) with get, set

    [<GlobalSetup>]
    member this.Setup() =
        let rnd = Random(42)
        let now = DateTimeOffset.UtcNow.ToUnixTimeSeconds()
        // Left base (orders)
        this.baseLeft <- Array.init this.DataSize (fun i ->
            {
                OrderId = i
                CustomerId = rnd.Next(this.DataSize / 10 + 1)
                ProductId = rnd.Next(10_000)
                Quantity = 1 + rnd.Next(5)
                PriceCents = 100 + rnd.Next(50_000)
                Timestamp = now - int64 (rnd.Next(0, 60*60*24*60))
            })
        // Right dimension (smaller in quick mode)
        let quick = String.Equals(Environment.GetEnvironmentVariable("DBSP_QUICK_BENCH"), "1", StringComparison.OrdinalIgnoreCase)
        let rightSize = if quick then 5_000 else 10_000
        this.baseRight <- Array.init rightSize (fun pid -> pid, if pid % 2 = 0 then "A" else "B")
        this.rightZ <- ZSet.ofSeq (seq { for (pid, cls) in this.baseRight -> ((pid, cls), 1) }) |> ZSet.mapKeys id

        // Preload operator states with base data (steady-state)
        // 1) Preload right side into join state
        let _ = (this.joinOp.EvalAsync ZSet.empty this.rightZ).Result in ()
        let _ = (this.fusedJoinProject.EvalAsync (ZSet.empty<int * (int * int)>) this.rightZ).Result in ()

        // 2) Feed base left through filter+map and into join to build left state
        let leftBaseZ = ZSet.ofSeq (seq { for o in this.baseLeft -> (o, 1) })
        let mapped = this.filterMap.EvalAsync(leftBaseZ).Result
        let _ = (this.joinOp.EvalAsync mapped ZSet.empty).Result in ()

        // 3) Optionally seed aggregator; skip in quick mode
        if not quick then
            let joinedDelta = (this.joinOp.EvalAsync ZSet.empty ZSet.empty).Result |> IndexedZSet.toZSet
            let toAgg = this.projectToAgg.EvalAsync(joinedDelta).Result
            let _ = this.sumAgg.EvalAsync(toAgg).Result in ()

        // Prepare change set
        this.changes <- Array.init this.ChangeCount (fun _ ->
            let idx = rnd.Next this.DataSize
            let o = this.baseLeft[idx]
            { o with Quantity = o.Quantity + rnd.Next(0, 2); PriceCents = o.PriceCents + rnd.Next(-50, 50) })

    // Baseline: naive recompute on arrays
    [<Benchmark(Baseline = true)>]
    member this.Naive_Recompute_All() =
        let cutoff = DateTimeOffset.UtcNow.ToUnixTimeSeconds() - int64 (60*60*24*30)
        // Apply changes
        let mutable orders = Array.copy this.baseLeft
        for n in this.changes do
            orders.[n.OrderId] <- n
        // Pipeline: filter->map->join->aggregate
        let filtered = orders |> Array.filter (fun o -> o.Timestamp >= cutoff)
        let mapped = filtered |> Array.map (fun o -> (o.ProductId, (o.CustomerId, o.Quantity * o.PriceCents)))
        let rightIdx = System.Collections.Generic.Dictionary<int,string>(this.baseRight.Length)
        for (pid, cls) in this.baseRight do rightIdx[pid] <- cls
        let joined =
            mapped
            |> Array.choose (fun (pid, (cid, amt)) ->
                match rightIdx.TryGetValue(pid) with
                | true, cls -> Some (cid, cls, amt)
                | _ -> None)
        joined
        |> Array.groupBy (fun (cid, cls, _) -> struct (cid, cls))
        |> Array.map (fun (k, arr) -> k, arr |> Array.sumBy (fun (_,_,amt) -> amt))
        |> Array.length

    // Incremental: delta propagation through operators
    [<Benchmark>]
    member this.Incremental_Circuit() =
        // Build change ZSet (-old + new)
        let delta =
            this.changes
            |> Array.collect (fun n ->
                let o = this.baseLeft[n.OrderId]
                [| (o, -1); (n, 1) |])
            |> Array.toSeq
            |> ZSet.ofSeq
        // Stage 1: filter+map delta
        let leftDelta = this.filterMap.EvalAsync(delta).Result
        // Stage 2: join delta with preloaded right state
        let joinDelta = (this.joinOp.EvalAsync leftDelta ZSet.empty).Result |> IndexedZSet.toZSet
        // Stage 3: project to aggregation key/value
        let toAgg = this.projectToAgg.EvalAsync(joinDelta).Result
        // Stage 4: incremental sum aggregate
        let _aggOut = this.sumAgg.EvalAsync(toAgg).Result
        // Return simple scalar
        1

    // Incremental using fused JoinProject
    [<Benchmark>]
    member this.Incremental_Circuit_FusedJoinProject() =
        let delta =
            this.changes
            |> Array.collect (fun n ->
                let o = this.baseLeft[n.OrderId]
                [| (o, -1); (n, 1) |])
            |> Array.toSeq
            |> ZSet.ofSeq
        let leftDelta = this.filterMap.EvalAsync(delta).Result
        let joinProj = (this.fusedJoinProject.EvalAsync leftDelta (ZSet.empty<int * string>)).Result
        let _aggOut = this.sumAgg.EvalAsync(joinProj).Result
        1
