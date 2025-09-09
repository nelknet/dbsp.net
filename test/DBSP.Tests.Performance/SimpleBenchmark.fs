/// Simple benchmark to verify Phase 5.1 optimizations
module DBSP.Tests.Performance.SimpleBenchmark

open System
open BenchmarkDotNet.Attributes
open BenchmarkDotNet.Diagnosers
open DBSP.Core.ZSet

[<MemoryDiagnoser>]
[<SimpleJob>]
type MemoryOptimizationTest() =
    
    [<Params(100, 500)>]
    member val Count = 0 with get, set

    [<Benchmark(Baseline = true)>]
    member this.Old_IncrementalPattern() =
        // The problematic pattern from benchmark data
        let mutable result = ZSet.empty<int>
        for i in 1 .. this.Count do
            let singleton = ZSet.singleton i 1
            result <- ZSet.add result singleton
        result
    
    [<Benchmark>]
    member this.New_BuilderPattern() =
        // Optimized builder pattern
        ZSet.buildZSet (fun builder ->
            for i in 1 .. this.Count do
                builder.Add(i, 1)
        )
    
    [<Benchmark>]
    member this.New_BatchPattern() =
        // Simple batch construction
        let pairs = [1 .. this.Count] |> List.map (fun i -> (i, 1))
        ZSet.ofList pairs