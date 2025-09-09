/// Fast-running benchmarks for Phase 5.1 analysis  
/// Designed to complete quickly while providing actionable performance data
module DBSP.Tests.Performance.FastBenchmarks

open System
open System.Collections.Generic
open BenchmarkDotNet.Attributes
open BenchmarkDotNet.Diagnosers
open FSharp.Data.Adaptive
open DBSP.Core.ZSet
open DBSP.Core.ZSetOptimized

/// Quick data structure comparison (completes in <30 seconds)
[<MemoryDiagnoser>]
[<SimpleJob>]
type QuickDataStructureBenchmarks() =
    
    [<Params(100, 1_000)>] // Smaller sizes for faster completion
    member val SetSize = 0 with get, set
    
    member val private testData: (int * int)[] = [||] with get, set
    
    [<GlobalSetup>]
    member this.Setup() =
        let random = Random(42)
        this.testData <- Array.init this.SetSize (fun i -> (i, random.Next(-10, 10)))

    [<Benchmark(Baseline = true)>]
    member this.HashMap_Union() =
        let map1 = HashMap.ofArray this.testData
        let map2 = HashMap.ofArray this.testData
        HashMap.unionWith (fun _ a b -> a + b) map1 map2
    
    [<Benchmark>]
    member this.FSharpMap_Union() =
        let map1 = Map.ofArray this.testData
        let map2 = Map.ofArray this.testData
        Map.fold (fun acc k v -> 
            match Map.tryFind k acc with
            | Some existing -> Map.add k (existing + v) acc
            | None -> Map.add k v acc
        ) map1 map2

/// Critical memory allocation analysis (the 44MB issue)
[<MemoryDiagnoser>]
[<SimpleJob>]
type MemoryHotspotBenchmarks() =
    
    [<Params(100, 500)>] // Keep small for fast completion
    member val OperationCount = 0 with get, set

    [<Benchmark(Baseline = true)>]
    member this.Current_IncrementalPattern() =
        // This is the problematic pattern causing 44MB allocations
        let mutable result = ZSet.empty<int>
        for i in 1 .. this.OperationCount do
            let singleton = ZSet.singleton i 1  // Creates new ZSet each iteration
            result <- ZSet.add result singleton
        result
    
    [<Benchmark>]
    member this.Optimized_BuilderPattern() =
        // Optimized pattern using builder to avoid intermediate allocations
        ZSetOptimized.buildZSet (fun builder ->
            for i in 1 .. this.OperationCount do
                builder.Add(i, 1)
        )
    
    [<Benchmark>]
    member this.Optimized_BatchConstruction() =
        // Direct batch construction
        let pairs = [1 .. this.OperationCount] |> List.map (fun i -> (i, 1))
        ZSet.ofList pairs

/// SRTP performance validation (fast version)
[<SimpleJob>]
type QuickSRTPBenchmarks() =
    
    [<Params(1_000)>]
    member val Count = 0 with get, set
    
    member val private testZSet: ZSet<int> = ZSet.empty with get, set
    
    [<GlobalSetup>]
    member this.Setup() =
        let testData = [1 .. this.Count] |> List.map (fun i -> (i, 1))
        this.testZSet <- ZSet.ofList testData

    [<Benchmark(Baseline = true)>]
    member this.SRTP_Addition() =
        this.testZSet + this.testZSet
        
    [<Benchmark>]
    member this.Direct_Addition() =
        ZSet.add this.testZSet this.testZSet

/// Benchmark runner that completes quickly
module QuickBenchmarkRunner =
    
    /// Run fast benchmarks for immediate feedback
    let runQuickAnalysis() =
        printfn "=== Quick Phase 5.1 Performance Analysis ===" 
        
        let results = [
            BenchmarkDotNet.Running.BenchmarkRunner.Run<QuickDataStructureBenchmarks>()
            BenchmarkDotNet.Running.BenchmarkRunner.Run<MemoryHotspotBenchmarks>()
            BenchmarkDotNet.Running.BenchmarkRunner.Run<QuickSRTPBenchmarks>()
        ]
        
        printfn "Quick benchmark analysis completed."
        results