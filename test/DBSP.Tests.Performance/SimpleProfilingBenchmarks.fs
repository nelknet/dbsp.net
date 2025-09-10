module DBSP.Tests.Performance.SimpleProfilingBenchmarks

open System
open BenchmarkDotNet.Attributes
open BenchmarkDotNet.Diagnosers
open DBSP.Core
open DBSP.Core.ZSet
open FSharp.Data.Adaptive

/// Simplified profiling benchmark to identify key bottlenecks
[<MemoryDiagnoser>]
[<EventPipeProfiler(EventPipeProfile.CpuSampling)>]
type CoreBottleneckBenchmarks() =
    
    let mutable baseData = [||]
    let mutable changes = [||]
    
    [<Params(1000, 10000, 50000)>]
    member val DataSize = 0 with get, set
    
    [<Params(100, 1000)>]
    member val ChangeSize = 0 with get, set
    
    [<GlobalSetup>]
    member this.Setup() =
        baseData <- [| for i in 1..this.DataSize -> (i, $"value_{i}") |]
        changes <- 
            [| for i in 1..this.ChangeSize do
                let id = Random.Shared.Next(1, this.DataSize + 1)
                yield (id, $"updated_{id}") |]
    
    [<Benchmark(Baseline = true)>]
    member this.Naive_Recalculation() =
        let changeMap = changes |> Map.ofArray
        baseData 
        |> Array.map (fun (id, value) ->
            match Map.tryFind id changeMap with
            | Some newValue -> (id, newValue)
            | None -> (id, value))
        |> Array.groupBy snd
        |> Array.length
    
    [<Benchmark>]
    member this.DBSP_ZSetOperations() =
        // Create base ZSet
        let baseZSet = ZSet.ofSeq (baseData |> Seq.map (fun kv -> (kv, 1)))
        
        // Create deletes for changed records
        let deletes = 
            changes 
            |> Array.choose (fun (id, _) ->
                baseData |> Array.tryFind (fun (i, _) -> i = id)
                |> Option.map (fun oldKv -> (oldKv, -1)))
        
        // Create inserts
        let inserts = changes |> Array.map (fun kv -> (kv, 1))
        
        // Build and apply delta
        let deltaZSet = ZSet.ofSeq (Seq.append deletes inserts)
        let result = ZSet.add baseZSet deltaZSet
        
        // Count results  
        result.Inner 
        |> HashMap.filter (fun _ weight -> weight <> 0)
        |> HashMap.count
    
    [<Benchmark>]
    member this.HashMap_DirectOperations() =
        // Direct HashMap operations to measure overhead
        let mutable map = HashMap.empty<int * string, int>
        
        // Add base data
        for kv in baseData do
            map <- HashMap.add kv 1 map
        
        // Apply changes
        for (id, newValue) in changes do
            // Remove old if exists
            match baseData |> Array.tryFind (fun (i, _) -> i = id) with
            | Some oldKv -> map <- HashMap.remove oldKv map
            | None -> ()
            // Add new
            map <- HashMap.add (id, newValue) 1 map
        
        HashMap.count map

/// Memory allocation analysis
[<MemoryDiagnoser>]
type AllocationAnalysis() =
    
    [<Params(100, 1000, 10000)>]
    member val Size = 0 with get, set
    
    [<Benchmark>]
    member this.ZSet_SingletonPattern() =
        // Anti-pattern: creating many singletons
        let mutable zset = ZSet.empty<int * string>
        for i in 1..this.Size do
            let single = ZSet.singleton (i, $"v{i}") 1
            zset <- ZSet.add zset single
        zset
    
    [<Benchmark>]
    member this.ZSet_BatchPattern() =
        // Optimized pattern: batch creation
        let pairs = [| for i in 1..this.Size -> ((i, $"v{i}"), 1) |]
        ZSet.ofSeq pairs
    
    [<Benchmark>]
    member this.HashMap_Incremental() =
        let mutable map = HashMap.empty<int, string>
        for i in 1..this.Size do
            map <- HashMap.add i $"v{i}" map
        map
    
    [<Benchmark>]
    member this.HashMap_Batch() =
        let pairs = [| for i in 1..this.Size -> (i, $"v{i}") |]
        HashMap.ofArray pairs