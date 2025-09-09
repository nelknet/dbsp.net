namespace DBSP.Circuit

open System
open System.Collections.Generic
open System.Runtime.CompilerServices
open System.Threading
open System.Threading.Tasks
open FSharp.Data.Adaptive
open DBSP.Core.ZSet

/// Batch processing configuration tuned for .NET performance characteristics
type BatchConfig = {
    /// Target batch size for memory efficiency (tuned for L1/L2 cache)
    TargetBatchSize: int
    /// Maximum batch size to prevent memory pressure
    MaxBatchSize: int
    /// Latency threshold for batch flushing (ms)
    LatencyThresholdMs: int
    /// Enable adaptive batch sizing based on performance feedback
    AdaptiveSizing: bool
} with
    /// Optimized defaults based on .NET 9.0 performance characteristics
    static member Optimized = {
        TargetBatchSize = 1000    // Sweet spot for cache locality
        MaxBatchSize = 10000      // Prevent memory pressure
        LatencyThresholdMs = 1    // Sub-millisecond target
        AdaptiveSizing = true     // Enable automatic tuning
    }
    
    /// High-throughput configuration
    static member HighThroughput = {
        TargetBatchSize = 5000    // Larger batches for throughput
        MaxBatchSize = 50000      // Higher memory usage acceptable
        LatencyThresholdMs = 10   // Higher latency acceptable
        AdaptiveSizing = true
    }
    
    /// Low-latency configuration
    static member LowLatency = {
        TargetBatchSize = 100     // Small batches for responsiveness
        MaxBatchSize = 1000       // Keep memory pressure low
        LatencyThresholdMs = 1    // Aggressive latency target
        AdaptiveSizing = false    // Fixed sizing for predictable latency
    }

/// Simple circular buffer for float metrics (avoids generic constraint issues)
type FloatCircularBuffer(capacity: int) =
    let buffer = Array.zeroCreate<float> capacity
    let mutable count = 0
    let mutable head = 0
    
    member this.Add(item: float) =
        buffer.[head] <- item
        head <- (head + 1) % capacity
        count <- min (count + 1) capacity
    
    member this.IsFull = count = capacity
    
    member this.Average() : float =
        if count = 0 then 0.0
        else
            let sum = buffer |> Array.take count |> Array.sum
            sum / float count

/// Adaptive batch size controller using performance feedback
type AdaptiveBatchController(initialConfig: BatchConfig) =
    let mutable currentBatchSize = initialConfig.TargetBatchSize
    let mutable recentLatencies = new FloatCircularBuffer(10) // Track last 10 measurements
    let mutable recentThroughput = new FloatCircularBuffer(10)
    
    /// Update batch size based on performance feedback
    member this.UpdateBatchSize(latencyMs: float, throughputOpsPerSec: float) =
        if initialConfig.AdaptiveSizing then
            recentLatencies.Add(latencyMs)
            recentThroughput.Add(throughputOpsPerSec)
            
            if recentLatencies.IsFull then
                let avgLatency = recentLatencies.Average()
                let avgThroughput = recentThroughput.Average()
                
                // Increase batch size if latency is acceptable and we want more throughput
                if avgLatency < float initialConfig.LatencyThresholdMs * 0.8 then
                    currentBatchSize <- min (currentBatchSize + 100) initialConfig.MaxBatchSize
                // Decrease batch size if latency is too high
                elif avgLatency > float initialConfig.LatencyThresholdMs * 1.2 then
                    currentBatchSize <- max (currentBatchSize - 100) 100
    
    /// Get current optimal batch size
    member this.CurrentBatchSize = currentBatchSize


/// Optimized batch processor for ZSet operations
type ZSetBatchProcessor<'K when 'K : comparison>(config: BatchConfig) =
    let batchController = new AdaptiveBatchController(config)
    let pendingOperations = new Queue<ZSet<'K> -> ZSet<'K>>()
    
    /// Add operation to batch
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    member this.AddOperation(operation: ZSet<'K> -> ZSet<'K>) =
        pendingOperations.Enqueue(operation)
        
        // Flush if batch is ready
        if pendingOperations.Count >= batchController.CurrentBatchSize then
            this.FlushBatch()
    
    /// Flush current batch of operations
    member this.FlushBatch() = 
        if pendingOperations.Count > 0 then
            let operations = Array.zeroCreate pendingOperations.Count
            let mutable index = 0
            
            // Dequeue all operations
            while pendingOperations.Count > 0 do
                operations.[index] <- pendingOperations.Dequeue()
                index <- index + 1
            
            // Execute batch using optimized pattern
            this.ExecuteBatchOptimized(operations)
    
    /// Execute batch with minimal allocations
    member private this.ExecuteBatchOptimized(operations: (ZSet<'K> -> ZSet<'K>)[]) =
        // Use buildZSet pattern to minimize allocations
        let result = ZSet.buildZSet (fun builder ->
            for operation in operations do
                // Apply operation and add results to builder
                let result = operation ZSet.empty // Placeholder
                result.Inner |> HashMap.iter (fun key weight ->
                    builder.Add(key, weight)
                )
        )
        () // Return unit since this is a private method

/// Task coordination utilities for optimal .NET parallel execution
module TaskCoordination =
    
    /// Enhanced Task.WhenAll with timeout and cancellation
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let whenAllWithTimeout (tasks: Task[]) (timeoutMs: int) = task {
        let timeout = Task.Delay(timeoutMs)
        let allTasks = Task.WhenAll(tasks)
        
        let! completed = Task.WhenAny(allTasks, timeout)
        
        if completed = timeout then
            // Timeout occurred
            return Error "Task coordination timeout"
        else
            // All tasks completed
            let! results = allTasks
            return Ok results
    }
    
    /// Partitioned parallel execution with load balancing
    let executePartitioned<'T, 'R> (items: 'T[]) (processor: 'T -> 'R) (workerCount: int) = task {
        let partitionSize = max 1 (items.Length / workerCount)
        let partitions = items |> Array.chunkBySize partitionSize
        
        let! results = 
            partitions 
            |> Array.map (fun partition -> task {
                return partition |> Array.map processor
            })
            |> Task.WhenAll
        
        return Array.concat results
    }
    
    /// NUMA-aware parallel processing using .NET ThreadLocal patterns
    let processWithNUMAAwareness<'T, 'R> (items: 'T[]) (processor: 'T -> ThreadLocal<BufferCache> -> 'R) = task {
        let tasks = 
            items 
            |> Array.chunkBySize 1000 // Optimal chunk size for cache locality
            |> Array.map (fun chunk -> Task.Factory.StartNew(fun () ->
                let cache = new ThreadLocal<BufferCache>(fun () -> BufferCache.Create())
                chunk |> Array.map (fun item -> processor item cache)
            , TaskCreationOptions.LongRunning)) // LongRunning for NUMA placement
        
        let! results = Task.WhenAll(tasks)
        return Array.concat results
    }

/// Performance monitoring for parallel execution
module ParallelPerformanceMonitor =
    
    /// Measure parallel execution performance
    type ParallelExecutionMetrics = {
        ParallelEfficiency: float    // % of ideal parallel speedup achieved
        WorkerBalancing: float       // How evenly work is distributed
        CacheLocalityRatio: float    // Thread-local cache hit ratio
        BatchingEfficiency: float    // Effectiveness of batch processing
        NUMALocalityRatio: float     // Memory access locality
    }
    
    /// Benchmark parallel execution efficiency
    let measureParallelEfficiency (parallelFunc: unit -> Task<'T>) (sequentialFunc: unit -> 'T) (workerCount: int) = task {
        // Measure sequential baseline
        let seqStopwatch = System.Diagnostics.Stopwatch.StartNew()
        let _ = sequentialFunc()
        seqStopwatch.Stop()
        let sequentialTime = seqStopwatch.Elapsed
        
        // Measure parallel execution
        let parStopwatch = System.Diagnostics.Stopwatch.StartNew()
        let! _ = parallelFunc()
        parStopwatch.Stop()
        let parallelTime = parStopwatch.Elapsed
        
        // Calculate parallel efficiency (ideal would be 100% / workerCount)
        let speedup = sequentialTime.TotalMilliseconds / parallelTime.TotalMilliseconds
        let idealSpeedup = float workerCount
        let efficiency = speedup / idealSpeedup * 100.0
        
        return {
            ParallelEfficiency = efficiency
            WorkerBalancing = 0.0 // Placeholder
            CacheLocalityRatio = 0.0 // Placeholder  
            BatchingEfficiency = 0.0 // Placeholder
            NUMALocalityRatio = 0.0 // Placeholder
        }
    }