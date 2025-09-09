/// Comprehensive allocation hotspot analysis for DBSP codebase
/// Identifies and benchmarks allocation-heavy patterns found in operators
module DBSP.Tests.Performance.AllocationHotspotBenchmarks

open System
open BenchmarkDotNet.Attributes
open BenchmarkDotNet.Diagnosers
open DBSP.Core.ZSet
open DBSP.Core.Collections.OptimizedCollections

[<MemoryDiagnoser>]
[<SimpleJob>]
type SeqCollectPatternBenchmarks() =
    
    [<Params(1_000, 5_000)>]
    member val Count = 0 with get, set
    
    member val private testZSets: ZSet<int * string>[] = [||] with get, set
    
    [<GlobalSetup>]
    member this.Setup() =
        let random = Random(42)
        this.testZSets <- 
            Array.init 10 (fun i ->
                let pairs = [1 .. this.Count / 10] |> List.map (fun j -> ((i * 100 + j, $"val_{j}"), 1))
                ZSet.ofList pairs)

    [<Benchmark(Baseline = true)>]
    member this.Current_SeqCollect_Pattern() =
        // Pattern found in ComplexJoinOperators.fs - creates many intermediate sequences
        this.testZSets
        |> Seq.collect (fun zset ->
            ZSet.toSeq zset
            |> Seq.collect (fun ((k, v), w) ->
                ZSet.toSeq zset  // Nested seq creation
                |> Seq.map (fun ((k2, v2), w2) -> ((k, v, k2, v2), w * w2))))
        |> ZSet.ofSeq
    
    [<Benchmark>]
    member this.Optimized_Builder_Pattern() =
        // Optimized version using builder to avoid intermediate sequence allocations
        ZSet.buildZSet (fun builder ->
            for zset in this.testZSets do
                for KeyValue((k, v), w) in zset.Inner do
                    for KeyValue((k2, v2), w2) in zset.Inner do
                        builder.Add((k, v, k2, v2), w * w2)
        )

[<MemoryDiagnoser>]  
[<SimpleJob>]
type TupleAllocationBenchmarks() =
    
    [<Params(10_000)>]
    member val Count = 0 with get, set

    [<Benchmark(Baseline = true)>]
    member this.Reference_Tuple_Creation() =
        // Current pattern: reference tuples cause heap allocation
        let mutable total = 0
        for i in 1 .. this.Count do
            let tuple = (i, i * 2, $"item_{i}")  // Reference tuple
            let (a, b, _) = tuple
            total <- total + a + b
        total
    
    [<Benchmark>]
    member this.Struct_Tuple_Creation() =
        // Optimized pattern: struct tuples avoid heap allocation
        let mutable total = 0
        for i in 1 .. this.Count do
            let struct(a, b, _) = struct(i, i * 2, $"item_{i}")  // Struct tuple
            total <- total + a + b
        total

[<MemoryDiagnoser>]
[<SimpleJob>] 
type OptionPatternBenchmarks() =
    
    [<Params(10_000)>]
    member val Count = 0 with get, set

    [<Benchmark(Baseline = true)>]
    member this.Standard_Option_Usage() =
        // Current Option usage - heap allocated
        let mutable total = 0
        for i in 1 .. this.Count do
            let opt = if i % 2 = 0 then Some i else None
            match opt with
            | Some value -> total <- total + value
            | None -> ()
        total
    
    [<Benchmark>]
    member this.ValueOption_Usage() =
        // Struct-based ValueOption - stack allocated
        let mutable total = 0
        for i in 1 .. this.Count do
            let opt = if i % 2 = 0 then StructOptimizedCollections.ValueOption<int>.Some(i) 
                     else StructOptimizedCollections.ValueOption<int>.None
            if opt.IsSome then
                total <- total + opt.Value
        total

[<MemoryDiagnoser>]
[<SimpleJob>]
type IndexedZSetPatternBenchmarks() =
    
    [<Params(1_000)>]
    member val SetSize = 0 with get, set
    
    member val private testData: (string * int)[] = [||] with get, set
    
    [<GlobalSetup>]
    member this.Setup() =
        this.testData <- Array.init this.SetSize (fun i -> ($"key_{i % 100}", i))

    [<Benchmark(Baseline = true)>]
    member this.Current_IndexedZSet_GroupBy() =
        // Current pattern from IndexedZSet.fs - uses ZSet.singleton in fold
        let zset = ZSet.ofArray this.testData
        HashMap.fold (fun acc value weight ->
            let key = fst value  // Extract grouping key
            HashMap.alter key (function
                | Some existing -> Some (ZSet.add existing (ZSet.singleton value weight))
                | None -> Some (ZSet.singleton value weight)  // Allocation hotspot!
            ) acc
        ) HashMap.empty zset.Inner
    
    [<Benchmark>]
    member this.Optimized_IndexedZSet_GroupBy() =
        // Optimized version using builder pattern
        let zset = ZSet.ofArray this.testData
        let resultBuilder = new System.Collections.Generic.Dictionary<string, ZSet.ZSetBuilder<string * int>>()
        
        for KeyValue(value, weight) in zset.Inner do
            let key = fst value
            match resultBuilder.TryGetValue(key) with
            | false, _ -> 
                let builder = ZSet.ZSetBuilder<string * int>()
                builder.Add(value, weight)
                resultBuilder.[key] <- builder
            | true, builder -> 
                builder.Add(value, weight)
        
        resultBuilder.Values
        |> Seq.map (fun builder -> builder.Build())
        |> Seq.fold ZSet.union ZSet.empty