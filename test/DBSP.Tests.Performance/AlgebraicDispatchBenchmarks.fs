/// SRTP vs Interface Performance Validation  
/// Comprehensive benchmarking of algebraic operation dispatch methods
/// Validates zero-cost abstraction claims for mathematical operations
module DBSP.Tests.Performance.AlgebraicDispatchBenchmarks

open System
open System.Runtime.CompilerServices
open BenchmarkDotNet.Attributes
open BenchmarkDotNet.Diagnosers
open DBSP.Core.ZSet
open DBSP.Core.Algebra

/// Interface-based algebraic operations for comparison
type IMonoidOperations<'T> =
    abstract member Zero: 'T
    abstract member Add: 'T -> 'T -> 'T
    abstract member Negate: 'T -> 'T

/// ZSet interface implementation for benchmark comparison
type ZSetMonoidOps<'K when 'K : comparison>() =
    interface IMonoidOperations<ZSet<'K>> with
        member _.Zero = ZSet.empty<'K>
        member _.Add left right = ZSet.add left right
        member _.Negate zset = ZSet.negate zset

/// Note: IWSAM (Interface with Static Abstract Members) benchmarks removed 
/// due to F# syntax limitations - focus on SRTP vs Interface comparison

/// Comprehensive SRTP vs Interface performance comparison
[<MemoryDiagnoser>]
[<HardwareCounters(HardwareCounter.BranchMispredictions, 
                   HardwareCounter.InstructionRetired,
                   HardwareCounter.CacheMisses)>]
[<SimpleJob>]
type AlgebraicDispatchBenchmarks() =
    
    [<Params(1_000, 10_000, 100_000)>]
    member val OperationCount = 0 with get, set
    
    member val private testZSet: ZSet<int> = ZSet.empty with get, set
    member val private monoidOps: IMonoidOperations<ZSet<int>> = Unchecked.defaultof<_> with get, set
    
    [<GlobalSetup>]
    member this.Setup() =
        let testData = [1 .. this.OperationCount] |> List.map (fun i -> (i, 1))
        this.testZSet <- ZSet.ofList testData
        this.monoidOps <- ZSetMonoidOps<int>() :> IMonoidOperations<ZSet<int>>

    /// BASELINE: SRTP with compile-time specialization
    [<Benchmark(Baseline = true)>]
    [<BenchmarkCategory("Addition")>]
    member this.SRTP_Addition() =
        // F# 7+ SRTP syntax - zero-cost at runtime
        this.testZSet + this.testZSet
        
    /// Interface dispatch with virtual call overhead
    [<Benchmark>]
    [<BenchmarkCategory("Addition")>]  
    member this.Interface_Addition() =
        this.monoidOps.Add this.testZSet this.testZSet
        
    /// Direct function call (should be similar to SRTP)
    [<Benchmark>]
    [<BenchmarkCategory("Addition")>]
    member this.Direct_Addition() =
        ZSet.add this.testZSet this.testZSet
        
    /// Inline function call for maximum performance
    [<Benchmark>]
    [<BenchmarkCategory("Addition")>]
    member this.Inline_Addition() =
        add this.testZSet this.testZSet

    /// SRTP negation operation
    [<Benchmark>]
    [<BenchmarkCategory("Negation")>]
    member this.SRTP_Negation() =
        -this.testZSet
        
    /// Interface negation
    [<Benchmark>]
    [<BenchmarkCategory("Negation")>]
    member this.Interface_Negation() =
        this.monoidOps.Negate this.testZSet
        
    /// Direct negation
    [<Benchmark>]
    [<BenchmarkCategory("Negation")>]
    member this.Direct_Negation() =
        ZSet.negate this.testZSet

/// Phase 5.1: Memory allocation pattern analysis
[<MemoryDiagnoser>]
[<ThreadingDiagnoser>]
type MemoryAllocationAnalysis() =
    
    [<Params(1_000, 10_000, 100_000)>]
    member val OperationCount = 0 with get, set

    /// Measure allocation in repeated ZSet operations
    [<Benchmark>]
    [<BenchmarkCategory("AllocationPatterns")>]
    member this.Incremental_ZSet_Construction() =
        let mutable result = ZSet.empty<int>
        for i in 1 .. this.OperationCount do
            let singleton = ZSet.singleton i 1
            result <- ZSet.add result singleton
        result
        
    /// Measure allocation in batch construction
    [<Benchmark>]
    [<BenchmarkCategory("AllocationPatterns")>]
    member this.Batch_ZSet_Construction() =
        let pairs = [1 .. this.OperationCount] |> List.map (fun i -> (i, 1))
        ZSet.ofList pairs
        
    /// Measure allocation in SRTP operations
    [<Benchmark>]
    [<BenchmarkCategory("AllocationPatterns")>]
    member this.SRTP_Chain_Operations() =
        let initial = ZSet.ofList [(1, 1); (2, 2); (3, 3)]
        let mutable result = initial
        for _ in 1 .. this.OperationCount / 100 do
            result <- result + (-result) + result
        result

/// Struct vs Reference type performance comparison
[<MemoryDiagnoser>]
[<SimpleJob>]
type StructVsRefTypeBenchmarks() =
    
    [<Params(10_000, 100_000)>]
    member val Count = 0 with get, set
    
    /// Struct-based pair operations
    [<Benchmark>]
    [<BenchmarkCategory("TypeComparison")>]
    member this.Struct_ValuePair_Operations() =
        let mutable total = 0
        for i in 1 .. this.Count do
            let struct(a, b) = struct(i, i * 2) // Struct tuple
            total <- total + a + b
        total
        
    /// Reference-based pair operations
    [<Benchmark>]
    [<BenchmarkCategory("TypeComparison")>]
    member this.Reference_Pair_Operations() =
        let mutable total = 0
        for i in 1 .. this.Count do
            let pair = (i, i * 2) // Reference tuple
            let (a, b) = pair
            total <- total + a + b
        total

/// Phase 5.1 benchmark runner with categorization
module Phase5_1_Runner =
    
    /// Run all Phase 5.1 benchmarks
    let runAllBenchmarks() =
        printfn "=== Phase 5.1: Data Structure Performance Optimization Benchmarks ===" 
        
        let results = [
            BenchmarkDotNet.Running.BenchmarkRunner.Run<AlgebraicDispatchBenchmarks>()
            BenchmarkDotNet.Running.BenchmarkRunner.Run<MemoryAllocationAnalysis>()
            BenchmarkDotNet.Running.BenchmarkRunner.Run<StructVsRefTypeBenchmarks>()
        ]
        
        printfn "Phase 5.1 benchmarking completed. Check BenchmarkDotNet.Artifacts for detailed results."
        results
    
    /// Run specific benchmark categories
    let runSRTPBenchmarks() = BenchmarkDotNet.Running.BenchmarkRunner.Run<AlgebraicDispatchBenchmarks>()
    let runMemoryBenchmarks() = BenchmarkDotNet.Running.BenchmarkRunner.Run<MemoryAllocationAnalysis>()
    let runStructBenchmarks() = BenchmarkDotNet.Running.BenchmarkRunner.Run<StructVsRefTypeBenchmarks>()