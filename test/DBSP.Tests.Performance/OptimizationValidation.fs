/// Validation benchmarks for Phase 5.1 optimizations
module DBSP.Tests.Performance.OptimizationValidation

open System
open BenchmarkDotNet.Attributes
open BenchmarkDotNet.Diagnosers
open DBSP.Core.ZSet

[<MemoryDiagnoser>]
[<SimpleJob>]
type BeforeAfterOptimizationBenchmarks() =
    
    [<Params(1000, 10000)>]
    member val OperationCount = 0 with get, set

    [<Benchmark(Baseline = true)>]
    member this.Before_IncrementalPattern() =
        // OLD: The pattern causing 44MB allocations
        let mutable result = ZSet.empty<int>
        for i in 1 .. this.OperationCount do
            let singleton = ZSet.singleton i 1
            result <- ZSet.add result singleton
        result
    
    [<Benchmark>]
    member this.After_BuilderPattern() =
        // NEW: Optimized builder pattern
        ZSet.buildZSet (fun builder ->
            for i in 1 .. this.OperationCount do
                builder.Add(i, 1)
        )
    
    [<Benchmark>]
    member this.After_BatchConstruction() =
        // NEW: Batch construction
        let pairs = [1 .. this.OperationCount] |> List.map (fun i -> (i, 1))
        ZSet.ofList pairs