module DBSP.Tests.Performance.ProfilingBenchmarks

open System
open System.Collections.Generic
open BenchmarkDotNet.Attributes
open BenchmarkDotNet.Diagnosers
open BenchmarkDotNet.Engines
open DBSP.Core
open FSharp.Data.Adaptive

/// Detailed profiling benchmark to identify performance bottlenecks
[<MemoryDiagnoser>]
[<ThreadingDiagnoser>]
[<EventPipeProfiler(EventPipeProfile.CpuSampling)>]
[<DisassemblyDiagnoser(maxDepth = 3)>]
type BottleneckIdentificationBenchmarks() =
    
    let mutable baseData: (int * string) array = [||]
    let mutable changes: (int * string) array = [||]
    let mutable baseZSet: ZSet<int * string> = ZSet.empty()
    let mutable changesZSet: ZSet<int * string> = ZSet.empty()
    
    [<Params(1000, 10000, 50000)>]
    member val DataSize = 0 with get, set
    
    [<Params(10, 100, 1000)>]
    member val ChangeSize = 0 with get, set
    
    [<GlobalSetup>]
    member this.Setup() =
        // Create base dataset
        baseData <- 
            [| for i in 1..this.DataSize do
                yield (i, $"value_{i}") |]
        
        // Create changes (updates to existing records)
        changes <- 
            [| for i in 1..this.ChangeSize do
                let id = Random.Shared.Next(1, this.DataSize + 1)
                yield (id, $"updated_{id}") |]
        
        // Pre-create ZSets for DBSP operations
        baseZSet <- ZSet.ofSeq (baseData |> Seq.map (fun kv -> (kv, 1)))
        changesZSet <- ZSet.ofSeq (changes |> Seq.map (fun kv -> (kv, 1)))
    
    // ============= Individual Operation Profiling =============
    
    [<Benchmark>]
    member this.Profile_ZSetCreation() =
        ZSet.ofSeq (baseData |> Seq.map (fun kv -> (kv, 1)))
    
    [<Benchmark>]
    member this.Profile_ZSetUnion() =
        ZSet.add baseZSet changesZSet
    
    [<Benchmark>]
    member this.Profile_ZSetSubtraction() =
        let toDelete = ZSet.ofSeq (changes |> Seq.map (fun kv -> (kv, -1)))
        ZSet.add baseZSet toDelete
    
    [<Benchmark>]
    member this.Profile_HashMapOperations() =
        let mutable map = HashMap.empty<int * string, int>
        for kv in baseData do
            map <- HashMap.add kv 1 map
        map
    
    [<Benchmark>]
    member this.Profile_HashMapUnion() =
        let map1 = baseData |> Seq.map (fun kv -> (kv, 1)) |> HashMap.ofSeq
        let map2 = changes |> Seq.map (fun kv -> (kv, 1)) |> HashMap.ofSeq
        HashMap.unionWith (fun _ v1 v2 -> v1 + v2) map1 map2
    
    // ============= Full Incremental Update Profiling =============
    
    [<Benchmark>]
    member this.Profile_IncrementalUpdate_Detailed() =
        // Step 1: Create delete operations for changed records
        let deletes = 
            changes 
            |> Array.map (fun (id, _) ->
                // Find original value
                match Array.tryFind (fun (i, _) -> i = id) baseData with
                | Some original -> (original, -1)
                | None -> ((id, ""), 0))
            |> Array.filter (fun (_, weight) -> weight <> 0)
        
        // Step 2: Create insert operations
        let inserts = changes |> Array.map (fun kv -> (kv, 1))
        
        // Step 3: Build delta ZSet
        let deltaZSet = 
            ZSet.ofSeq (Seq.append deletes inserts)
        
        // Step 4: Apply delta to base
        ZSet.add baseZSet deltaZSet

/// Allocation-focused profiling to identify memory bottlenecks
[<MemoryDiagnoser>]
[<DisassemblyDiagnoser>]
type AllocationProfilingBenchmarks() =
    
    [<Params(100, 1000, 10000)>]
    member val Size = 0 with get, set
    
    [<Benchmark>]
    member this.Allocation_ZSetOfSeq() =
        let data = seq { for i in 1..this.Size -> ((i, $"v{i}"), 1) }
        ZSet.ofSeq data
    
    [<Benchmark>]
    member this.Allocation_ZSetOfList() =
        let data = [ for i in 1..this.Size -> ((i, $"v{i}"), 1) ]
        ZSet.ofList data
    
    [<Benchmark>]
    member this.Allocation_ZSetOfArray() =
        let data = [| for i in 1..this.Size -> ((i, $"v{i}"), 1) |]
        ZSet.ofSeq data
    
    [<Benchmark>]
    member this.Allocation_RepeatedSingletonAdd() =
        let mutable zset = ZSet.empty<int * string>()
        for i in 1..this.Size do
            let singleton = ZSet.singleton (i, $"v{i}") 1
            zset <- ZSet.add zset singleton
        zset
    
    [<Benchmark>]
    member this.Allocation_BatchAdd() =
        let pairs = [| for i in 1..this.Size -> ((i, $"v{i}"), 1) |]
        ZSet.ofSeq pairs

/// Interface dispatch vs direct call profiling
[<DisassemblyDiagnoser>]
type DispatchProfilingBenchmarks() =
    
    let testZSet = ZSet.ofList [((1, "a"), 1); ((2, "b"), 2)]
    
    [<Benchmark(Baseline = true)>]
    member _.DirectCall() =
        ZSet.add testZSet testZSet
    
    [<Benchmark>]
    member _.InterfaceCall() =
        let monoid = testZSet :> IMonoid<ZSet<int * string>>
        monoid.Combine(testZSet)
    
    [<Benchmark>]
    member _.SRTPCall() =
        let inline add (x: ^T when ^T : (static member (+) : ^T * ^T -> ^T)) y =
            (^T : (static member (+) : ^T * ^T -> ^T) (x, y))
        add testZSet testZSet

/// HashMap-specific performance profiling
[<MemoryDiagnoser>]
type HashMapProfilingBenchmarks() =
    
    [<Params(100, 1000, 10000)>]
    member val Size = 0 with get, set
    
    let mutable hashMap = HashMap.empty<int, string>
    let mutable fsharpMap = Map.empty<int, string>
    let mutable dict = Dictionary<int, string>()
    
    [<GlobalSetup>]
    member this.Setup() =
        let data = [| for i in 1..this.Size -> (i, $"value_{i}") |]
        hashMap <- HashMap.ofArray data
        fsharpMap <- Map.ofArray data
        dict <- Dictionary<int, string>(data)
    
    [<Benchmark(Baseline = true)>]
    member this.HashMap_Lookup() =
        let mutable sum = 0
        for i in 1..100 do
            let key = Random.Shared.Next(1, this.Size + 1)
            match HashMap.tryFind key hashMap with
            | Some _ -> sum <- sum + 1
            | None -> ()
        sum
    
    [<Benchmark>]
    member this.FSharpMap_Lookup() =
        let mutable sum = 0
        for i in 1..100 do
            let key = Random.Shared.Next(1, this.Size + 1)
            match Map.tryFind key fsharpMap with
            | Some _ -> sum <- sum + 1
            | None -> ()
        sum
    
    [<Benchmark>]
    member this.Dictionary_Lookup() =
        let mutable sum = 0
        for i in 1..100 do
            let key = Random.Shared.Next(1, this.Size + 1)
            if dict.ContainsKey(key) then
                sum <- sum + 1
        sum
    
    [<Benchmark>]
    member this.HashMap_Union() =
        let other = HashMap.ofArray [| for i in this.Size..(this.Size + 99) -> (i, $"new_{i}") |]
        HashMap.union hashMap other
    
    [<Benchmark>]
    member this.HashMap_Iteration() =
        let mutable count = 0
        for KeyValue(_, _) in hashMap do
            count <- count + 1
        count