/// Comprehensive Data Structure Performance Benchmarking
/// Comparing HashMap vs FastDict vs F# Map vs ImmutableDictionary for DBSP operations
/// Validates architectural decisions and identifies optimization opportunities
module DBSP.Tests.Performance.DataStructureComparisonBenchmarks

open System
open System.Collections.Generic
open System.Collections.Immutable
open BenchmarkDotNet.Attributes
open BenchmarkDotNet.Diagnosers
open BenchmarkDotNet.Running
open FSharp.Data.Adaptive
open DBSP.Core.ZSet
open DBSP.Core.Collections.FastZSet

/// Test data generation for benchmarks
module TestDataGeneration =
    
    /// Generate deterministic test data for integer keys
    let generateIntData (size: int) (seed: int) =
        let random = Random(seed)
        Array.init size (fun i -> (i, random.Next(-100, 100)))
    
    /// Generate deterministic test data for string keys  
    let generateStringData (size: int) (seed: int) =
        let random = Random(seed)
        Array.init size (fun i -> 
            (sprintf "key_%d_%d" i (random.Next()), random.Next(-100, 100)))
    
    /// Generate heavy collision test data
    let generateCollisionData (size: int) =
        Array.init size (fun i -> (i % 10, i)) // Many collisions

/// Phase 5.1.1: HashMap vs FastDict Core Operations Benchmarking
[<MemoryDiagnoser>]
[<HardwareCounters(HardwareCounter.CacheMisses, 
                   HardwareCounter.BranchInstructions,
                   HardwareCounter.BranchMispredictions)>]
[<SimpleJob>]
type DataStructureCoreBenchmarks() =
    
    [<Params(100, 1_000, 10_000, 100_000)>]
    member val SetSize = 0 with get, set
    
    [<Params(42, 123, 789)>] // Different seeds for statistical significance
    member val Seed = 0 with get, set
    
    member val private intTestData: (int * int)[] = [||] with get, set
    member val private stringTestData: (string * int)[] = [||] with get, set
    member val private collisionTestData: (int * int)[] = [||] with get, set
    
    [<GlobalSetup>]
    member this.Setup() =
        this.intTestData <- TestDataGeneration.generateIntData this.SetSize this.Seed
        this.stringTestData <- TestDataGeneration.generateStringData this.SetSize this.Seed
        this.collisionTestData <- TestDataGeneration.generateCollisionData this.SetSize

    // ==================== INTEGER KEY BENCHMARKS ====================
    
    [<Benchmark(Baseline = true)>]
    [<BenchmarkCategory("IntUnion")>]
    member this.FSharpMap_IntUnion() =
        let map1 = Map.ofArray this.intTestData
        let map2 = Map.ofArray this.intTestData
        Map.fold (fun acc k v -> 
            match Map.tryFind k acc with
            | Some existing -> Map.add k (existing + v) acc
            | None -> Map.add k v acc
        ) map1 map2
    
    [<Benchmark>]
    [<BenchmarkCategory("IntUnion")>]
    member this.HashMap_IntUnion() =
        let map1 = HashMap.ofArray this.intTestData
        let map2 = HashMap.ofArray this.intTestData  
        HashMap.unionWith (fun _ a b -> a + b) map1 map2
    
    [<Benchmark>]
    [<BenchmarkCategory("IntUnion")>]
    member this.FastZSet_IntUnion() =
        let dict1 = FastZSet.ofList (List.ofArray this.intTestData)
        let dict2 = FastZSet.ofList (List.ofArray this.intTestData)
        FastZSet.union dict1 dict2
    
    [<Benchmark>]
    [<BenchmarkCategory("IntUnion")>]
    member this.ImmutableDict_IntUnion() =
        let dict1 = this.intTestData |> Array.map KeyValuePair |> ImmutableDictionary.CreateRange
        let dict2 = this.intTestData |> Array.map KeyValuePair |> ImmutableDictionary.CreateRange
        // ImmutableDictionary doesn't have efficient union, simulate
        let builder = dict1.ToBuilder()
        builder.AddRange(dict2) |> ignore
        builder.ToImmutable()

    // ==================== STRING KEY BENCHMARKS ====================
    
    [<Benchmark(Baseline = true)>]
    [<BenchmarkCategory("StringUnion")>]
    member this.FSharpMap_StringUnion() =
        let map1 = Map.ofArray this.stringTestData
        let map2 = Map.ofArray this.stringTestData
        Map.fold (fun acc k v -> 
            match Map.tryFind k acc with
            | Some existing -> Map.add k (existing + v) acc
            | None -> Map.add k v acc
        ) map1 map2
    
    [<Benchmark>]
    [<BenchmarkCategory("StringUnion")>]
    member this.HashMap_StringUnion() =
        let map1 = HashMap.ofArray this.stringTestData
        let map2 = HashMap.ofArray this.stringTestData
        HashMap.unionWith (fun _ a b -> a + b) map1 map2
    
    [<Benchmark>]
    [<BenchmarkCategory("StringUnion")>]
    member this.FastZSet_StringUnion() =
        let dict1 = FastZSet.ofList (List.ofArray this.stringTestData)
        let dict2 = FastZSet.ofList (List.ofArray this.stringTestData)
        FastZSet.union dict1 dict2

    // ==================== LOOKUP BENCHMARKS ====================
    
    [<Benchmark(Baseline = true)>]
    [<BenchmarkCategory("IntLookup")>]
    member this.FSharpMap_IntLookup() =
        let map = Map.ofArray this.intTestData
        let mutable total = 0
        for i in 0 .. this.SetSize - 1 do
            match Map.tryFind i map with
            | Some value -> total <- total + value
            | None -> ()
        total
    
    [<Benchmark>]
    [<BenchmarkCategory("IntLookup")>]
    member this.HashMap_IntLookup() =
        let map = HashMap.ofArray this.intTestData
        let mutable total = 0
        for i in 0 .. this.SetSize - 1 do
            match HashMap.tryFind i map with
            | Some value -> total <- total + value  
            | None -> ()
        total
    
    [<Benchmark>]
    [<BenchmarkCategory("IntLookup")>]
    member this.FastZSet_IntLookup() =
        let dict = FastZSet.ofList (List.ofArray this.intTestData)
        let mutable total = 0
        for i in 0 .. this.SetSize - 1 do
            let weight = FastZSet.tryGetWeight dict i
            total <- total + weight
        total

/// Phase 5.1.2: ZSet Operation Benchmarking
[<MemoryDiagnoser>]
[<HardwareCounters(HardwareCounter.CacheMisses, HardwareCounter.BranchMispredictions)>]
type ZSetOperationBenchmarks() =
    
    [<Params(100, 1_000, 10_000)>]
    member val SetSize = 0 with get, set
    
    member val private testData: (int * int)[] = [||] with get, set
    member val private zsetHashMap: ZSet<int> = ZSet.empty with get, set
    member val private zsetFast: FastZSet<int> = FastZSet.create() with get, set
    
    [<GlobalSetup>]
    member this.Setup() =
        this.testData <- TestDataGeneration.generateIntData this.SetSize 42
        this.zsetHashMap <- ZSet.ofList (List.ofArray this.testData)
        this.zsetFast <- FastZSet.ofList (List.ofArray this.testData)

    [<Benchmark(Baseline = true)>]
    [<BenchmarkCategory("ZSetAddition")>]
    member this.ZSet_Addition() =
        ZSet.add this.zsetHashMap this.zsetHashMap
    
    [<Benchmark>]  
    [<BenchmarkCategory("ZSetAddition")>]
    member this.FastZSet_Addition() =
        FastZSet.union this.zsetFast this.zsetFast
    
    [<Benchmark(Baseline = true)>]
    [<BenchmarkCategory("ZSetNegation")>]
    member this.ZSet_Negation() =
        ZSet.negate this.zsetHashMap
        
    [<Benchmark>]
    [<BenchmarkCategory("ZSetIteration")>]
    member this.ZSet_Iteration() =
        let mutable total = 0
        for (key, weight) in ZSet.toSeq this.zsetHashMap do
            total <- total + key + weight
        total
    
    [<Benchmark>]
    [<BenchmarkCategory("ZSetIteration")>]
    member this.FastZSet_Iteration() =
        let mutable total = 0
        for (key, weight) in FastZSet.toSeq this.zsetFast do
            total <- total + key + weight
        total

/// Phase 5.1.3: SRTP vs Interface Performance Validation
[<MemoryDiagnoser>]
[<SimpleJob>]
type SRTPPerformanceBenchmarks() =
    
    [<Params(1_000, 10_000)>]
    member val SetSize = 0 with get, set
    
    member val private testZSet: ZSet<int> = ZSet.empty with get, set
    
    [<GlobalSetup>]
    member this.Setup() =
        let testData = TestDataGeneration.generateIntData this.SetSize 42
        this.testZSet <- ZSet.ofList (List.ofArray testData)

    [<Benchmark(Baseline = true)>]
    [<BenchmarkCategory("AlgebraicOperations")>]
    member this.SRTP_Addition() =
        // F# 7+ SRTP syntax with compile-time specialization
        this.testZSet + this.testZSet
        
    [<Benchmark>]
    [<BenchmarkCategory("AlgebraicOperations")>]
    member this.Direct_Addition() =
        // Direct method call
        ZSet.add this.testZSet this.testZSet
        
    [<Benchmark(Baseline = true)>]
    [<BenchmarkCategory("AlgebraicOperations")>]
    member this.SRTP_ScalarMultiply() =
        5 * this.testZSet
        
    [<Benchmark>]
    [<BenchmarkCategory("AlgebraicOperations")>]
    member this.Direct_ScalarMultiply() =
        ZSet.scalarMultiply 5 this.testZSet

/// Phase 5.1.4: Memory Allocation Analysis
[<MemoryDiagnoser>]
[<ThreadingDiagnoser>]
type MemoryAllocationBenchmarks() =
    
    [<Params(1_000, 10_000)>]
    member val OperationCount = 0 with get, set

    [<Benchmark(Baseline = true)>]
    [<BenchmarkCategory("AllocationPatterns")>]
    member this.ZSet_RepeatedAdditions() =
        let mutable result = ZSet.empty<int>
        for i in 1 .. this.OperationCount do
            let zset = ZSet.singleton i 1
            result <- ZSet.add result zset
        result
    
    [<Benchmark>]
    [<BenchmarkCategory("AllocationPatterns")>]
    member this.ZSet_BatchConstruction() =
        let pairs = [1 .. this.OperationCount] |> List.map (fun i -> (i, 1))
        ZSet.ofList pairs

/// Phase 5.1.5: Cache Locality and Branch Prediction Optimization
[<MemoryDiagnoser>]
[<HardwareCounters(HardwareCounter.CacheMisses,
                   HardwareCounter.InstructionRetired,
                   HardwareCounter.BranchMispredictions,
                   HardwareCounter.LlcMisses)>]
type CacheLocalityBenchmarks() =
    
    [<Params(1_000, 10_000, 100_000)>]
    member val SetSize = 0 with get, set
    
    member val private sequentialData: (int * int)[] = [||] with get, set
    member val private randomData: (int * int)[] = [||] with get, set
    
    [<GlobalSetup>]
    member this.Setup() =
        // Sequential access pattern (cache-friendly)
        this.sequentialData <- Array.init this.SetSize (fun i -> (i, i))
        
        // Random access pattern (cache-hostile)  
        let random = Random(42)
        this.randomData <- 
            Array.init this.SetSize (fun _ -> (random.Next(), random.Next()))
    
    [<Benchmark(Baseline = true)>]
    [<BenchmarkCategory("CacheLocality")>]
    member this.HashMap_SequentialAccess() =
        let map = HashMap.ofArray this.sequentialData
        let mutable total = 0
        for (key, _) in this.sequentialData do
            total <- total + HashMap.find key map
        total
    
    [<Benchmark>]
    [<BenchmarkCategory("CacheLocality")>]
    member this.HashMap_RandomAccess() =
        let map = HashMap.ofArray this.randomData
        let mutable total = 0
        for (key, _) in this.randomData do
            total <- total + HashMap.find key map
        total
    
    [<Benchmark>]
    [<BenchmarkCategory("CacheLocality")>]
    member this.FastZSet_SequentialAccess() =
        let dict = FastZSet.ofList (List.ofArray this.sequentialData)
        let mutable total = 0
        for (key, _) in this.sequentialData do
            total <- total + FastZSet.tryGetWeight dict key
        total

/// Configuration for running Phase 5.1 benchmarks
module BenchmarkRunner =
    
    /// Run all Phase 5.1 benchmarks
    let runAll() =
        [
            BenchmarkRunner.Run<DataStructureCoreBenchmarks>()
            BenchmarkRunner.Run<ZSetOperationBenchmarks>()  
            BenchmarkRunner.Run<SRTPPerformanceBenchmarks>()
            BenchmarkRunner.Run<MemoryAllocationBenchmarks>()
            BenchmarkRunner.Run<CacheLocalityBenchmarks>()
        ]
    
    /// Run specific benchmark category
    let runDataStructure() = BenchmarkRunner.Run<DataStructureCoreBenchmarks>()
    let runZSetOperations() = BenchmarkRunner.Run<ZSetOperationBenchmarks>()
    let runSRTP() = BenchmarkRunner.Run<SRTPPerformanceBenchmarks>()
    let runMemoryAllocation() = BenchmarkRunner.Run<MemoryAllocationBenchmarks>()
    let runCacheLocality() = BenchmarkRunner.Run<CacheLocalityBenchmarks>()