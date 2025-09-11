namespace DBSP.Tests.Performance

open System
open BenchmarkDotNet.Attributes
open BenchmarkDotNet.Jobs
open DBSP.Core.ZSet

/// Simple benchmark showing DBSP incremental advantage
[<SimpleJob(RuntimeMoniker.Net90)>]
[<MemoryDiagnoser>]
type SimpleIncrementalBenchmark() =
    
    [<Params(1000, 5000)>]
    member val DataSize = 0 with get, set
    
    [<Params(10, 100)>]
    member val ChangeSize = 0 with get, set
    
    member val private baseData: (int * string) array = [||] with get, set
    member val private changes: (int * string) array = [||] with get, set
    
    [<GlobalSetup>]
    member this.Setup() =
        let rnd = Random(42)
        let categories = [|"A"; "B"; "C"|]
        
        // Generate base dataset
        this.baseData <- Array.init this.DataSize (fun i -> 
            (i, categories.[rnd.Next(categories.Length)]))
        
        // Generate changes 
        this.changes <- Array.init this.ChangeSize (fun i ->
            let existingId = rnd.Next(0, this.DataSize)
            (existingId, categories.[rnd.Next(categories.Length)] + "_NEW"))
    
    /// BASELINE: Naive approach - process everything
    [<Benchmark(Baseline = true)>]
    member this.NaiveRecalculation() =
        let changeMap = Map.ofArray this.changes
        let updated = 
            this.baseData 
            |> Array.map (fun (id, value) ->
                match Map.tryFind id changeMap with
                | Some newValue -> (id, newValue)
                | None -> (id, value))
        
        // Aggregate by value (simulate real work)
        updated |> Array.groupBy snd |> Array.length
    
    /// DBSP: Process only the deltas
    [<Benchmark>]
    member this.DBSPIncremental() =
        // Create base Z-set
        let baseZSet = ZSet.ofSeq [for (id, value) in this.baseData -> ((id, value), 1)]
        
        // Create change Z-set (delete old, insert new)
        let changeZSet = 
            ZSet.ofSeq [
                for (id, newValue) in this.changes do
                    let (_, oldValue) = this.baseData.[id]
                    yield ((id, oldValue), -1)  // Remove old
                    yield ((id, newValue), 1)   // Add new
            ]
        
        // Apply incremental update
        let result = ZSet.add baseZSet changeZSet
        
        // Process result (same aggregation)
        result 
        |> ZSet.toSeq 
        |> Seq.filter (fun (_, weight) -> weight > 0)
        |> Seq.map (fun ((_, value), _) -> value)
        |> Seq.groupBy id
        |> Seq.length