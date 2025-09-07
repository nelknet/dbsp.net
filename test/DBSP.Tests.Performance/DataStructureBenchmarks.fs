/// BenchmarkDotNet performance tests for core DBSP data structures  
/// Comparing FSharp.Data.Adaptive HashMap vs F# Map performance
module DBSP.Tests.Performance.DataStructureBenchmarks

open BenchmarkDotNet.Attributes
open BenchmarkDotNet.Running
open FSharp.Data.Adaptive
open DBSP.Core.ZSet
open System.Collections.Generic

[<MemoryDiagnoser>]
[<SimpleJob>]
type DataStructureComparisonBenchmarks() =
    
    [<Params(100, 1_000, 10_000)>]
    member val SetSize = 0 with get, set
    
    member val private testDataInt: (int * int) array = [||] with get, set
    member val private testDataString: (string * int) array = [||] with get, set
    
    [<GlobalSetup>]
    member this.Setup() =
        let random = System.Random(42) // Deterministic seed
        this.testDataInt <- 
            Array.init this.SetSize (fun i -> (i, random.Next(-100, 100)))
        this.testDataString <- 
            Array.init this.SetSize (fun i -> 
                (sprintf "key_%d" i, random.Next(-100, 100)))

    [<Benchmark(Baseline = true)>]
    member this.FSharpMap_IntUnion() =
        let map1 = Map.ofArray this.testDataInt
        let map2 = Map.ofArray this.testDataInt
        // Combine maps by adding values for duplicate keys
        Map.fold (fun acc k v -> 
            match Map.tryFind k acc with
            | Some existing -> Map.add k (existing + v) acc
            | None -> Map.add k v acc
        ) map1 map2
    
    [<Benchmark>] 
    member this.HashMap_IntUnion() =
        let map1 = HashMap.ofArray this.testDataInt
        let map2 = HashMap.ofArray this.testDataInt
        HashMap.unionWith (fun _ a b -> a + b) map1 map2

    [<Benchmark>]
    member this.FSharpMap_StringUnion() =
        let map1 = Map.ofArray this.testDataString
        let map2 = Map.ofArray this.testDataString
        Map.fold (fun acc k v -> 
            match Map.tryFind k acc with
            | Some existing -> Map.add k (existing + v) acc
            | None -> Map.add k v acc
        ) map1 map2

    [<Benchmark>]
    member this.HashMap_StringUnion() =
        let map1 = HashMap.ofArray this.testDataString
        let map2 = HashMap.ofArray this.testDataString  
        HashMap.unionWith (fun _ a b -> a + b) map1 map2

[<MemoryDiagnoser>]
type ZSetOperationBenchmarks() =
    
    [<Params(100, 1_000, 10_000)>]
    member val SetSize = 0 with get, set
    
    member val private zset1: ZSet<int> = ZSet<int>.Zero with get, set
    member val private zset2: ZSet<int> = ZSet<int>.Zero with get, set
    
    [<GlobalSetup>]
    member this.Setup() =
        let random = System.Random(42)
        let data1 = Array.init this.SetSize (fun i -> (i, random.Next(-5, 6)))
        let data2 = Array.init this.SetSize (fun i -> (i + this.SetSize/2, random.Next(-5, 6)))
        this.zset1 <- ZSet.ofList (Array.toList data1)
        this.zset2 <- ZSet.ofList (Array.toList data2)
    
    [<Benchmark>]
    member this.ZSet_Addition() =
        ZSet.add this.zset1 this.zset2
        
    [<Benchmark>]
    member this.ZSet_Negation() =
        ZSet.negate this.zset1
        
    [<Benchmark>]
    member this.ZSet_Union() =
        ZSet.union this.zset1 this.zset2

/// Entry point for running benchmarks
[<EntryPoint>]
let main args =
    let switcher = BenchmarkSwitcher [|
        typeof<DataStructureComparisonBenchmarks>
        typeof<ZSetOperationBenchmarks>
    |]
    switcher.Run(args) |> ignore
    0