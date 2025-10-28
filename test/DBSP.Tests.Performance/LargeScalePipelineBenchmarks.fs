namespace DBSP.Tests.Performance

open System
open BenchmarkDotNet.Attributes
open BenchmarkDotNet.Jobs
open DBSP.Core.ZSet

/// End-to-end comparison: naive full recompute vs incremental DBSP on large data
[<MemoryDiagnoser>]
// Synthetic record matching typical map/filter/join pipelines
type Order = {
    OrderId: int
    CustomerId: int
    ProductId: int
    Quantity: int
    PriceCents: int
    Timestamp: int64
}

type LargeScalePipelineBenchmarks() =
    
    // Parameters aligned with user scenario
    member this.DataSizes =
        let quick = System.String.Equals(System.Environment.GetEnvironmentVariable("DBSP_QUICK_BENCH"), "1", System.StringComparison.OrdinalIgnoreCase)
        if quick then [| 100000; 300000 |] else [| 100000; 1000000 |]
    [<ParamsSource("DataSizes")>]
    member val DataSize = 0 with get, set

    // Small change sizes to probe crossover point
    member this.ChangeCounts =
        let quick = System.String.Equals(System.Environment.GetEnvironmentVariable("DBSP_QUICK_BENCH"), "1", System.StringComparison.OrdinalIgnoreCase)
        if quick then [| 1; 100 |] else [| 1; 10; 100; 1000 |]
    [<ParamsSource("ChangeCounts")>]
    member val ChangeCount = 0 with get, set

    // Generated data and mutable state for incremental scenario
    member val private baseLeft : Order array = Array.empty with get, set
    member val private baseRight: (int * string) array = Array.empty with get, set
    member val private changes  : Order array = Array.empty with get, set
    member val private stateZ   : ZSet<Order> = ZSet.empty with get, set

    // Precomputed dictionaries for naive baseline joins/aggregations
    member val private rightIndex : System.Collections.Generic.Dictionary<int, string> = new System.Collections.Generic.Dictionary<int,string>() with get, set

    [<GlobalSetup>]
    member this.Setup() =
        let rnd = Random(42)
        let now = DateTimeOffset.UtcNow.ToUnixTimeSeconds()
        // Base left dataset (1M scale)
        this.baseLeft <- Array.init this.DataSize (fun i ->
            {
                OrderId = i
                CustomerId = rnd.Next(this.DataSize / 10 + 1)
                ProductId = rnd.Next(10_000)
                Quantity = 1 + rnd.Next(5)
                PriceCents = 100 + rnd.Next(50_000)
                Timestamp = now - int64 (rnd.Next(0, 60*60*24*60)) // last 60 days
            })

        // Base right side (e.g., product dimension)
        this.baseRight <- Array.init 10_000 (fun pid -> pid, (if pid % 2 = 0 then "A" else "B"))
        let ri = System.Collections.Generic.Dictionary<int,string>(this.baseRight.Length)
        for (pid, cls) in this.baseRight do ri[pid] <- cls
        this.rightIndex <- ri

        // Small set of changes (mutations to existing orders)
        this.changes <- Array.init this.ChangeCount (fun _ ->
            let idx = rnd.Next this.DataSize
            let o = this.baseLeft[idx]
            { o with Quantity = o.Quantity + (rnd.Next(0,2)); PriceCents = o.PriceCents + rnd.Next(-50,50) })

        // Initialize incremental state
        this.stateZ <- ZSet.ofSeq (seq { for o in this.baseLeft -> (o, 1) })

    // Baseline: naive full recomputation of a small pipeline
    // Pipeline: filter recent -> map amount -> join product class -> aggregate per customer
    [<Benchmark(Baseline = true)>]
    member this.Naive_Recompute_All() =
        let cutoff = DateTimeOffset.UtcNow.ToUnixTimeSeconds() - int64 (60*60*24*30) // 30 days
        let filtered = this.baseLeft |> Array.filter (fun o -> o.Timestamp >= cutoff)
        // Map to (cust, pid, amount)
        let mapped = filtered |> Array.map (fun o -> (o.CustomerId, o.ProductId, o.Quantity * o.PriceCents))
        // Join on product to get class
        let joined =
            mapped
            |> Array.choose (fun (cid, pid, amt) ->
                match this.rightIndex.TryGetValue(pid) with
                | true, cls -> Some (cid, cls, amt)
                | _ -> None)
        // Aggregate total amount per (customer, class)
        joined
        |> Array.groupBy (fun (cid, cls, _) -> struct (cid, cls))
        |> Array.map (fun (k, arr) -> k, arr |> Array.sumBy (fun (_,_,amt) -> amt))
        |> Array.length

    // Incremental: update ZSet state with deltas and run same logical pipeline using ZSet ops
    [<Benchmark>]
    member this.DBSP_Incremental_ApplyOnly() =
        // Build change ZSet as (-old + new) updates
        let delta =
            this.changes
            |> Array.collect (fun n ->
                let o = this.baseLeft[n.OrderId]
                [| (o, -1); (n, 1) |])
            |> Array.toSeq
            |> ZSet.ofSeq
        this.stateZ <- ZSet.add this.stateZ delta
        // Return count to keep work alive
        this.stateZ |> ZSet.toSeq |> Seq.length

    // Incremental pipeline execution using ZSet operators as a proxy
    [<Benchmark>]
    member this.DBSP_Incremental_Pipeline() =
        let cutoff = DateTimeOffset.UtcNow.ToUnixTimeSeconds() - int64 (60*60*24*30)
        // Build delta
        let delta =
            this.changes
            |> Array.collect (fun n ->
                let o = this.baseLeft[n.OrderId]
                [| (o, -1); (n, 1) |])
            |> Array.toSeq
            |> ZSet.ofSeq
        // Apply delta
        this.stateZ <- ZSet.add this.stateZ delta
        // Evaluate pipeline against current state (approximation)
        this.stateZ
        |> ZSet.filter (fun o -> o.Timestamp >= cutoff)
        |> ZSet.mapKeys (fun o -> (o.CustomerId, o.ProductId, o.Quantity * o.PriceCents))
        |> ZSet.toSeq
        |> Seq.length
