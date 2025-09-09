/// Phase 5.2: Parallel Execution Performance Benchmarks
/// Validates .NET-native parallel optimization patterns and worker coordination
module DBSP.Tests.Performance.ParallelExecutionBenchmarks

open System
open System.Threading
open System.Threading.Tasks
open BenchmarkDotNet.Attributes
open BenchmarkDotNet.Diagnosers
open DBSP.Core.ZSet
open DBSP.Circuit
open System.Collections.Generic

/// Thread coordination pattern benchmarks
[<MemoryDiagnoser>]
[<ThreadingDiagnoser>]
[<SimpleJob>]
type TaskCoordinationBenchmarks() =
    
    [<Params(1, 2, 4, 8)>]
    member val WorkerCount = 0 with get, set
    
    [<Params(1_000, 10_000)>]
    member val WorkloadSize = 0 with get, set
    
    member val private testData: int[] = [||] with get, set
    
    [<GlobalSetup>]
    member this.Setup() =
        this.testData <- Array.init this.WorkloadSize id

    [<Benchmark(Baseline = true)>]
    member this.Sequential_Processing() =
        let mutable total = 0
        for item in this.testData do
            total <- total + (item * item) // Simulate computational work
        total
    
    [<Benchmark>]
    member this.Parallel_TaskWhenAll() = task {
        let chunkSize = max 1 (this.testData.Length / this.WorkerCount)
        let chunks = this.testData |> Array.chunkBySize chunkSize
        
        let! results = 
            chunks
            |> Array.map (fun chunk -> task {
                let mutable total = 0
                for item in chunk do
                    total <- total + (item * item)
                return total
            })
            |> Task.WhenAll
        
        return Array.sum results
    }
    
    [<Benchmark>]
    member this.Parallel_LongRunningTasks() = task {
        let chunkSize = max 1 (this.testData.Length / this.WorkerCount)
        let chunks = this.testData |> Array.chunkBySize chunkSize
        
        let! results = 
            chunks
            |> Array.map (fun chunk -> 
                Task.Factory.StartNew(fun () ->
                    let mutable total = 0
                    for item in chunk do
                        total <- total + (item * item)
                    total
                , TaskCreationOptions.LongRunning))
            |> Task.WhenAll
        
        return Array.sum results
    }

/// ThreadLocal optimization benchmarks
[<MemoryDiagnoser>]
[<SimpleJob>]
type ThreadLocalOptimizationBenchmarks() =
    
    /// Shared cache (potential contention)
    let sharedCache = System.Collections.Concurrent.ConcurrentDictionary<int, obj>()
    
    /// Thread-local cache (NUMA-optimized)
    let threadLocalCache = new ThreadLocal<Dictionary<int, obj>>(fun () -> Dictionary<int, obj>())
    
    [<Params(4, 8)>]
    member val WorkerCount = 0 with get, set
    
    [<Params(10_000)>]
    member val OperationCount = 0 with get, set

    [<Benchmark(Baseline = true)>]
    member this.Shared_Cache_Access() = task {
        let tasks = 
            Array.init this.WorkerCount (fun workerId -> task {
                let mutable operations = 0
                for i in 1 .. (this.OperationCount / this.WorkerCount) do
                    let key = workerId * 1000 + i
                    sharedCache.TryAdd(key, box i) |> ignore
                    operations <- operations + 1
                return operations
            })
        
        let! results = Task.WhenAll(tasks)
        return Array.sum results
    }
    
    [<Benchmark>]
    member this.ThreadLocal_Cache_Access() = task {
        let tasks = 
            Array.init this.WorkerCount (fun workerId -> 
                Task.Factory.StartNew(fun () ->
                    let cache = threadLocalCache.Value
                    let mutable operations = 0
                    for i in 1 .. (this.OperationCount / this.WorkerCount) do
                        let key = workerId * 1000 + i
                        cache.[key] <- box i
                        operations <- operations + 1
                    operations
                , TaskCreationOptions.LongRunning))
        
        let! results = Task.WhenAll(tasks)
        return Array.sum results
    }

/// ZSet parallel processing benchmarks
[<MemoryDiagnoser>]
[<SimpleJob>]
type ZSetParallelProcessingBenchmarks() =
    
    [<Params(1, 2, 4)>]
    member val WorkerCount = 0 with get, set
    
    [<Params(10_000)>]
    member val ZSetSize = 0 with get, set
    
    member val private testZSets: ZSet<int>[] = [||] with get, set
    
    [<GlobalSetup>]
    member this.Setup() =
        this.testZSets <- 
            Array.init this.WorkerCount (fun i ->
                let pairs = [i * this.ZSetSize .. (i + 1) * this.ZSetSize - 1] 
                           |> List.map (fun j -> (j, 1))
                ZSet.ofList pairs)

    [<Benchmark(Baseline = true)>]
    member this.Sequential_ZSet_Union() =
        let mutable result = ZSet.empty<int>
        for zset in this.testZSets do
            result <- ZSet.union result zset
        result
    
    [<Benchmark>]
    member this.Parallel_ZSet_Union() = task {
        // Use parallel reduction pattern
        let! result = 
            this.testZSets
            |> Array.chunkBySize (max 1 (this.testZSets.Length / this.WorkerCount))
            |> Array.map (fun chunk -> task {
                let mutable localResult = ZSet.empty<int>
                for zset in chunk do
                    localResult <- ZSet.union localResult zset
                return localResult
            })
            |> Task.WhenAll
        
        // Final reduction
        return Array.fold ZSet.union ZSet.empty result
    }
    
    [<Benchmark>]
    member this.Builder_Parallel_ZSet_Union() = task {
        // Use optimized builder pattern for parallel union
        let! builderTasks = 
            this.testZSets
            |> Array.chunkBySize (max 1 (this.testZSets.Length / this.WorkerCount))
            |> Array.map (fun chunk -> task {
                return ZSet.buildZSet (fun builder ->
                    for zset in chunk do
                        FSharp.Data.Adaptive.HashMap.iter (fun key weight ->
                            builder.Add(key, weight)
                        ) zset.Inner
                )
            })
            |> Task.WhenAll
        
        // Combine results
        return Array.fold ZSet.union ZSet.empty builderTasks
    }

/// Batch size optimization benchmarks
[<MemoryDiagnoser>]
[<SimpleJob>]
type BatchSizeOptimizationBenchmarks() =
    
    [<Params(100, 1_000, 5_000, 10_000)>]
    member val BatchSize = 0 with get, set
    
    member val private operations: (int -> int)[] = [||] with get, set
    
    [<GlobalSetup>]
    member this.Setup() =
        this.operations <- Array.init 50000 (fun i -> fun x -> x + i)

    [<Benchmark>]
    member this.Batched_Operation_Processing() =
        let batches = this.operations |> Array.chunkBySize this.BatchSize
        let mutable totalResult = 0
        
        for batch in batches do
            let mutable batchResult = 0
            for operation in batch do
                batchResult <- batchResult + operation(1)
            totalResult <- totalResult + batchResult
        
        totalResult

/// Phase 5.2 benchmark runner
module Phase5_2_BenchmarkRunner =
    
    /// Run all parallel execution benchmarks
    let runAllParallelBenchmarks() =
        printfn "=== Phase 5.2: Parallel Execution and Runtime Optimization ===" 
        
        let results = [
            BenchmarkDotNet.Running.BenchmarkRunner.Run<TaskCoordinationBenchmarks>()
            BenchmarkDotNet.Running.BenchmarkRunner.Run<ThreadLocalOptimizationBenchmarks>() 
            BenchmarkDotNet.Running.BenchmarkRunner.Run<ZSetParallelProcessingBenchmarks>()
            BenchmarkDotNet.Running.BenchmarkRunner.Run<BatchSizeOptimizationBenchmarks>()
        ]
        
        printfn "Phase 5.2 parallel execution benchmarks completed."
        results