# Phase 5.3: Persistent Storage Backend Implementation - Technical Plan

## Executive Summary

Phase 5.3 implements a high-performance persistent storage backend for DBSP.NET, enabling operators to spill data to disk when memory is exhausted. We align with Feldera’s Batch/Trace/Spine model: immutable batches (no time), a monotonic Trace appending batches at non-decreasing logical times, and a multi-level Spine for merging/compaction. This phase focuses on the core storage engine, serialization abstractions, and memory management. Checkpointing and fault tolerance are deferred to Phase 5.4.

## Key Design Decisions

### 1. Storage Engine Choice: ZoneTree-Based Custom Implementation

**Decision**: Build a custom storage layer on top of ZoneTree’s LSM foundation, adapted to DBSP’s Batch/Trace/Spine semantics.

**Rationale**:
- **Feldera's lesson**: RocksDB failed due to threading model mismatches and deserialization overhead
- **ZoneTree advantages**: 
  - Native .NET implementation with zero-copy potential
  - Lock-free operations with configurable concurrency models
  - Built-in WAL, transactions, and compaction
  - Proven performance (millions of ops/sec)
- **DBSP adaptations needed**:
  - Z-set semantics (weight addition/cancellation)
  - Batches are timeless; Trace tracks time externally
  - Batch-oriented API for operator state and merges

### 2. Serialization Strategy: Pluggable Architecture

**Decision**: Implement a pluggable serialization interface. Start with MessagePack + FSharpResolver; evaluate MemoryPack for zero-allocation paths.

**Rationale**:
- Pluggable design: swap serializers without breaking public APIs
- MessagePack + FSharpResolver: solid baseline, good F# DU/record support, optional LZ4
- MemoryPack (optional): faster zero-allocation serializer for POD-like types
- Future: custom binary codecs for hot paths (fixed-width types, struct records)
- Note: true zero-copy (Rust rkyv-style) isn’t available in .NET; target minimal copies via Span/Memory APIs

### 3. Architecture: Shared-Nothing Per-Worker Storage

**Decision**: Each worker maintains an independent storage backend (per-worker directory and ZoneTree instance). Coordination happens at the circuit level.

**Rationale**:
- **Feldera architecture validation**: Shared-nothing scales linearly
- **Thread-local benefits**:
  - No lock contention on storage operations
  - NUMA-friendly memory access patterns
  - Independent failure domains
- **Coordination mechanism**:
  - Circuit-level checkpointing synchronization
  - Consistent snapshots across workers

## Detailed Technical Architecture

### Alignment with Feldera (Batch/Trace/Spine)

- Batch: immutable collection of (key, value, weight) tuples; zero weights eliminated; no time inside the batch.
- Trace: append-only sequence of batches with non-decreasing logical time; exposes read cursors and merge operations.
- Spine: multi-level organization of batches; merges reduce read/merge cost and eliminate zero-weight tuples.

Implications for .NET/ZoneTree:
- Do not encode time in the LSM key. Time lives in Trace/Spine metadata.
- Prefer composite key (K,V) as the ZoneTree key and store weight (int64) as the value.
- Write batches in bulk; let background compaction run; avoid frequent per-key commits.

### Core Storage Abstractions

```fsharp
namespace DBSP.Storage

open System
open System.Threading.Tasks
open System.Collections.Generic
open DBSP.Core.ZSet
open FSharp.Data.Adaptive
open Tenray.ZoneTree

/// Phase 5.3: Core storage abstractions for DBSP operators
module StorageCore =
    
    /// Pluggable serialization interface
    type ISerializer<'T> =
        /// Serialize value to bytes
        abstract member Serialize: value:'T -> byte[]
        
        /// Deserialize bytes to value
        abstract member Deserialize: bytes:byte[] -> 'T
        
        /// Get serialized size estimate
        abstract member EstimateSize: value:'T -> int
    
    /// Storage location hint for batch creation
    type BatchLocation =
        | InMemory           // Keep batch in memory
        | OnDisk             // Write directly to disk
        | Adaptive of int64  // Spill to disk if size exceeds threshold
    
    /// Batch storage trait - unified interface for memory and disk batches
    type IBatch<'K, 'V when 'K: comparison> =
        inherit IDisposable
        
        /// Get total number of entries
        abstract member Count: int64
        
        /// Estimated memory footprint in bytes
        abstract member SizeInBytes: int64
        
        /// Iterate over entries in key order (Task-based, not async)
        abstract member GetIterator: unit -> Task<seq<'K * 'V * int64>>
        
        /// Seek to specific key and iterate from there
        abstract member SeekFrom: key:'K -> Task<seq<'K * 'V * int64>>
        
        /// Range query between keys
        abstract member RangeQuery: startKey:'K -> endKey:'K -> Task<seq<'K * 'V * int64>>
    
    /// Trace storage for temporal data
    type ITrace<'K, 'V, 'T when 'K: comparison and 'T: comparison> =
        inherit IDisposable
        
        /// Insert batch at specific time
        abstract member InsertBatch: time:'T -> batch:IBatch<'K, 'V> -> Task<unit>
        
        /// Query at specific time point
        abstract member QueryAtTime: time:'T -> IBatch<'K, 'V>
        
        /// Compact trace up to frontier time
        abstract member CompactTo: frontier:'T -> Task<unit>
        
        /// Get current storage statistics
        abstract member GetStats: unit -> TraceStats
    
    /// Statistics for monitoring storage usage
    type TraceStats = {
        MemoryBatches: int
        DiskBatches: int
        TotalEntries: int64
        MemoryBytes: int64
        DiskBytes: int64
        CompactionsPending: int
    }
```

### LSM Tree Storage Implementation

```fsharp
/// LSM tree-based persistent storage using ZoneTree
module LsmStorage =
    
    open Tenray.ZoneTree
    open Tenray.ZoneTree.Options
    open MessagePack
    open MessagePack.FSharp
    
    /// MessagePack serializer implementation with F# support
    type MessagePackSerializer<'T>() =
        let options = 
            MessagePack.Resolvers.CompositeResolver.Create(
                FSharpResolver.Instance,
                MessagePack.Resolvers.StandardResolver.Instance
            ) |> MessagePackSerializerOptions.Standard.WithResolver
        
        interface ISerializer<'T> with
            member _.Serialize(value: 'T) = 
                MessagePackSerializer.Serialize(value, options)
            
            member _.Deserialize(bytes: byte[]) = 
                MessagePackSerializer.Deserialize<'T>(bytes, options)
            
            member _.EstimateSize(value: 'T) = 
                MessagePackSerializer.Serialize(value, options).Length
    
    /// Composite (key,value) as key; value holds Z-weight
    [<Struct>]
    type KV<'K,'V when 'K: comparison and 'V: comparison> = { K: 'K; V: 'V }

    /// LSM-backed batch implementation
    type LsmBatch<'K, 'V when 'K: comparison and 'V: comparison>(
        zoneTree: IZoneTree<KV<'K,'V>, int64>) =
        
        let mutable disposed = false
        
        interface IBatch<'K, 'V> with
            member _.Count = zoneTree.Count() |> int64
            
            member _.SizeInBytes =
                // Rough estimate: key+value per entry (serializer-specific in practice)
                int64 (zoneTree.Count()) * 32L
            
            member _.GetIterator() = task {
                let it = zoneTree.CreateIterator()
                it.SeekFirst() |> ignore
                return seq {
                    while it.IsValid do
                        let cur = it.Current
                        yield (cur.Key.K, cur.Key.V, cur.Value)
                        it.Next() |> ignore
                }
            }
            
            member _.SeekFrom(key: 'K) = task {
                let it = zoneTree.CreateIterator()
                it.Seek({ K = key; V = Unchecked.defaultof<'V> }) |> ignore
                return seq {
                    while it.IsValid do
                        let cur = it.Current
                        if cur.Key.K < key then it.Next() |> ignore
                        else
                            yield (cur.Key.K, cur.Key.V, cur.Value)
                            it.Next() |> ignore
                }
            }
            
            member _.RangeQuery(startKey: 'K, endKey: 'K) = task {
                let it = zoneTree.CreateIterator()
                it.Seek({ K = startKey; V = Unchecked.defaultof<'V> }) |> ignore
                return seq {
                    while it.IsValid do
                        let cur = it.Current
                        if cur.Key.K > endKey then ()
                        else
                            yield (cur.Key.K, cur.Key.V, cur.Value)
                            it.Next() |> ignore
                }
            }
        
        interface IDisposable with
            member _.Dispose() =
                if not disposed then
                    disposed <- true
                    // ZoneTree handles its own disposal
```

### Spilling Strategy

```fsharp
/// Memory pressure-aware spilling logic
module SpillingStrategy =
    
    open System.Runtime
    
    /// Global memory pressure monitor using GC.GetGCMemoryInfo()
    type MemoryMonitor(initialThreshold: float) =
        let mutable spillThreshold = initialThreshold // e.g., 0.8 for 80%
        member _.ShouldSpill(estimatedBytes: int64) =
            let info = GC.GetGCMemoryInfo()
            let used = GC.GetTotalMemory(false)
            let total = info.TotalAvailableMemoryBytes
            let pressure = float used / float total
            pressure >= spillThreshold || float estimatedBytes >= float total * 0.05
        member _.AdjustThreshold(gen2CollectionsDelta: int) =
            if gen2CollectionsDelta > 5 then spillThreshold <- max 0.5 (spillThreshold - 0.05)
            else spillThreshold <- min 0.9 (spillThreshold + 0.01)
    
    /// Per-worker spilling coordinator
    type SpillCoordinator(workerId: int, storageDir: string) =
        let monitor = MemoryMonitor(0.8)
        let spillPath = Path.Combine(storageDir, $"worker_{workerId}")
        
        /// Decide where to place next batch
        member _.PickDestination(batchSizeBytes: int64, hint: BatchLocation) =
            match hint with
            | InMemory -> 
                if monitor.ShouldSpill(batchSizeBytes) then OnDisk
                else InMemory
            | OnDisk -> OnDisk
            | Adaptive threshold ->
                if batchSizeBytes > threshold || monitor.ShouldSpill(batchSizeBytes) then
                    OnDisk
                else
                    InMemory
```

// Note: Checkpointing functionality moved to Phase 5.4 (Fault Tolerance)

### Temporal Trace Implementation

```fsharp
/// Temporal trace storage for incremental computation
module TemporalTrace =
    
    /// Multi-version trace using spine structure
    type SpineTrace<'K, 'V when 'K: comparison>(
        storageDir: string,
        compactionRatio: float) =
        
        // Spine levels with exponentially growing capacity
        let levels = [|
            for i in 0..8 ->
                let capacity = pown 2 (i + 3) // 8, 16, 32, ... 2048
                let path = Path.Combine(storageDir, $"level_{i}")
                new SpineLevel<'K, 'V>(path, capacity)
        |]
        
        let mutable currentTime = 0L
        let mutable pendingBatches = Queue<TimedBatch<'K, 'V>>()
        
        /// Insert batch at specific time
        member _.InsertBatch(time: int64, batch: IBatch<'K, 'V>) = task {
            currentTime <- max currentTime time
            
            // Add to pending batches
            let timedBatch = { Time = time; Batch = batch }
            pendingBatches.Enqueue(timedBatch)
            
            // Trigger merge if level 0 is full
            if pendingBatches.Count >= levels.[0].Capacity then
                do! _.MergeLevel(0)
        }
        
        /// Merge batches in a level
        member private _.MergeLevel(level: int) = task {
            if level >= levels.Length then
                return () // Overflow handling
            else
                let targetLevel = levels.[level]
                
                // Collect batches to merge
                let mergeCandidates = 
                    if level = 0 then
                        // Drain pending batches
                        let result = pendingBatches.ToArray()
                        pendingBatches.Clear()
                        result
                    else
                        // Get batches from this level
                        targetLevel.GetMergeCandidates(compactionRatio)
                
                if mergeCandidates.Length >= targetLevel.MergeThreshold then
                    // Perform merge
                    let! merged = _.MergeBatches(mergeCandidates)
                    
                    // Place merged batch in next level
                    if level + 1 < levels.Length then
                        do! levels.[level + 1].AddBatch(merged)
                        
                        // Cascade merge if needed
                        if levels.[level + 1].NeedsMerge() then
                            do! _.MergeLevel(level + 1)
        }
        
        /// Merge multiple batches using Z-set semantics
        member private _.MergeBatches(batches: TimedBatch<'K, 'V>[]) = task {
            // Create merge iterators (optimized: single map operation)
            let! iteratorTasks = 
                batches 
                |> Array.map (fun b -> task {
                    let! iter = b.Batch.GetIterator()
                    return iter.GetEnumerator()
                })
                |> Task.WhenAll
            
            // Aggregate weights across iterators (pairwise merge also acceptable)
            let mutable acc = HashMap.empty<'K, 'V * int64>
            for it in iteratorTasks do
                while it.MoveNext() do
                    let (k, v, w) = it.Current
                    acc <-
                        match HashMap.tryFind k acc with
                        | Some (_, w0) -> HashMap.add k (v, w0 + w) acc
                        | None -> HashMap.add k (v, w) acc
            let filtered =
                acc
                |> HashMap.toSeq
                |> Seq.choose (fun (k,(v,w)) -> if w = 0L then None else Some (k,v,w))
                |> Seq.toArray
            // Create new batch from merged entries
            return _.CreateBatchFromEntries(filtered)
        }
```

### Integration with Circuit Runtime

```fsharp
/// Circuit integration for operator state persistence
module CircuitIntegration =
    
    /// Extended operator interface with storage support
    type IStorageOperator<'I, 'O> =
        inherit IOperator
        
        /// Get current operator state size
        abstract member GetStateSize: unit -> int64
        
        /// Hint for batch placement
        abstract member GetStorageHint: unit -> BatchLocation
        
        /// Spill operator state to storage
        abstract member SpillState: storage:IBatch<'K, 'V> -> Task<unit>
    
    /// Storage-aware circuit runtime
    type StorageAwareRuntime(circuit: CircuitDefinition, storageConfig: StorageConfig) =
        inherit ParallelCircuitRuntime(circuit, storageConfig.RuntimeConfig)
        
        let storage = new DistributedStorage(storageConfig)
        let spillCoordinator = new SpillCoordinator(0, storageConfig.StorageDir)
        
        /// Execute step with storage awareness
        override _.ExecuteStepWithStorage() = task {
            // Monitor memory pressure
            let memoryPressure = GC.GetTotalMemory(false) / (1024L * 1024L * 1024L)
            
            if memoryPressure > storageConfig.SpillThresholdGB then
                // Trigger spilling for large operators
                for operator in circuit.Operators do
                    match operator with
                    | :? IStorageOperator as storageOp ->
                        let hint = storageOp.GetStorageHint()
                        let stateSize = storageOp.GetStateSize()
                        
                        // Check if we should spill this operator
                        let destination = spillCoordinator.PickDestination(stateSize, hint)
                        if destination = OnDisk then
                            do! storage.SpillOperatorState(operator.Id, storageOp)
                    | _ -> ()
            
            // Execute normal step
            return! base.ExecuteStep()
        }
```

## Performance Optimizations

### 1. Zero-Copy Deserialization

```fsharp
/// Zero-copy techniques for F#
module ZeroCopy =
    
    open System.Runtime.InteropServices
    open System.Buffers
    
    /// Fixed-size struct for zero-copy
    [<Struct; StructLayout(LayoutKind.Sequential, Pack = 1)>]
    type ZSetEntryFixed = {
        Key: int64
        Weight: int64
        Timestamp: int64
    }
    
    /// Read struct directly from memory
    let inline readStruct<'T when 'T: struct> (buffer: ReadOnlySpan<byte>) =
        MemoryMarshal.Read<'T>(buffer)
    
    /// Write struct directly to memory
    let inline writeStruct<'T when 'T: struct> (buffer: Span<byte>) (value: 'T) =
        MemoryMarshal.Write(buffer, &value)
```

### 2. Batch Processing

```fsharp
/// Optimized batch operations
module BatchOptimizations =
    
    /// Process Z-set operations in batches
    let processBatch (operations: ZSetOp<'K>[]) =
        // Sort operations by key for better cache locality
        let sorted = operations |> Array.sortBy (fun op -> op.Key)
        
        // Group operations by key
        let grouped = 
            sorted 
            |> Array.groupBy (fun op -> op.Key)
            |> Array.map (fun (key, ops) ->
                // Aggregate weights for same key
                let totalWeight = ops |> Array.sumBy (fun op -> op.Weight)
                (key, totalWeight)
            )
        
        // Filter zero-weight entries
        grouped |> Array.filter (fun (_, w) -> w <> 0L)
```

### 3. Compression

```fsharp
/// Compression for storage efficiency
module Compression =
    
    open MessagePack
    open MessagePack.Resolvers
    open MessagePack.Formatters
    
    /// Configure MessagePack with LZ4 compression
    let getCompressedSerializer() =
        let options = 
            MessagePackSerializerOptions.Standard
                .WithCompression(MessagePackCompression.Lz4BlockArray)
        MessagePackSerializer.DefaultOptions <- options
        MessagePackSerializer.Get<'T>()
```

## Testing Strategy

Add conformance tests against Feldera semantics:
- Trace monotonicity: inserting a batch with an earlier time is rejected.
- Merge equivalence: pairwise vs multi-way merge yield identical results.
- Zero-weight elimination: no zero weights remain post-merge/compaction.

### Unit Tests

```fsharp
// LSM batch correctly handles Z-set semantics (weight aggregation + zero removal)
[<Test>]
let ``LSM batch handles Z-set semantics`` () = task {
    use! backend = createZoneTreeBackend testDir
    do! backend.StoreBatch([ ("key1","v", 1L); ("key1","v", -1L); ("key2","v", 2L) ])
    let! iter = backend.GetRangeIterator(Some "key1", Some "key3")
    let results = iter |> Seq.toArray
    Assert.AreEqual(1, results.Length)
    Assert.AreEqual(("key2","v", 2L), results.[0])
}

[<Test>]
let ``Spilling to disk maintains correctness`` () = task {
    let runtime = new StorageAwareRuntime(circuit, config)
    
    // Fill memory to trigger spilling
    let largeData = Array.init 1000000 (fun i -> (i, i * 2))
    let batch = ZSet.ofArray largeData
    
    // Force spilling
    let! spilledBatch = runtime.SpillBatch(batch, OnDisk)
    
    // Verify we can read back the data
    let! retrieved = spilledBatch.GetIterator()
    let retrievedArray = retrieved |> Seq.toArray
    
    // Data should match
    Assert.AreEqual(largeData.Length, retrievedArray.Length)
}
```

### Performance Benchmarks

```fsharp
[<Benchmark>]
member _.ZoneTreeWrite() = task {
    let tree = new ZoneTree<int64, byte[]>(options)
    for i in 1L..1000000L do
        tree.Upsert(i, data)
    do! tree.CommitAsync()
}

[<Benchmark>]
member _.MessagePackSerialize() =
    let data = createTestZSet 10000
    MessagePackSerializer.Serialize(data)

[<Benchmark>]
member _.SpillToDisk() = task {
    let batch = createLargeBatch 1_000_000
    do! storage.SpillBatch(batch)
}
```

## Implementation Phases

### Phase 1: Core Storage Abstractions (Week 1)
- [x] Define storage interfaces with pluggable serialization (Serializer + Storage backends)
- [x] Implement in-memory storage using HashMap (weight aggregation + zero elimination)
- [x] Create serialization abstraction (ISerializer) with MessagePack default
- [x] Set up MessagePack.FSharpExtensions dependency (ZoneTree planned)

### Phase 2: LSM Storage Implementation (Week 2)
- [x] Implement LSM over ZoneTree<KV<'K,'V>, int64> (composite key)
- [x] Add WAL/commit hooks and background compaction integration (ZoneTree compaction wired; explicit merge invoked post-batch; tests green)
- [x] Implement Z-set semantics (weight aggregation, zero-weight elimination)
- [x] Provide streaming iterators and range queries

### Phase 3: Spilling and Memory Management (Week 3)
- [x] Implement memory pressure monitoring (GC.GetGCMemoryInfo)
- [x] Add spilling coordinator with adaptive thresholds
- [x] Create fallback in-memory storage (hybrid placeholder)
- [x] Test memory pressure scenarios

### Phase 4: Temporal Traces (Week 4)
- [x] Implement minimal Spine (time-indexed batches)
- [x] Add leveled merges/compaction; eliminate zeros (within-bucket compaction implemented; preserves per-time queryability)
- [x] Expose temporal queries by selecting batches by time

### Phase 5: Circuit Integration (Week 5)
- [x] Extend operators with storage hooks (state size estimate, spill trigger)
- [x] Integrate spilling with runtime; trigger based on GC pressure (StorageIntegration.forceSpill)
- [x] Add memory monitoring hooks and metrics (StorageIntegration.getMemoryPressure)
- [x] Test operator state spilling correctness (unit test with fake operator)

### Phase 6: Optimization and Testing (Week 6)
- [x] Profile serialization performance (added storage benchmarks)
- [x] Add LZ4 compression support (MessagePackCompressedSerializer)
- [x] Unit + property tests for semantics; CI-friendly output
- [x] Performance benchmarking against targets (StorageBenchmarks)

## Risk Mitigation

### Technical Risks

1. **Serializer Fit**
   - Risk: MessagePack perf/allocations on hot paths
   - Mitigation: Allow MemoryPack/custom codecs for POD types
   - Fallback: FsPickler for complex F# types

2. **ZoneTree Performance**
   - Risk: May not match Feldera’s custom LSM
   - Mitigation: Use composite key + weight value; batch writes + background compaction
   - Fallback: File-backed append-only batches with custom leveled merge if required

3. **Memory Pressure Detection**
   - Risk: GC metrics may lag
   - Mitigation: GC.GetGCMemoryInfo + conservative thresholds; expose manual spill API
   - Fallback: Hard caps on per-worker in-memory batch sizes

## Success Metrics

- **Performance**: <10ms latency for checkpoint creation
- **Throughput**: >1M ops/sec for in-memory operations
- **Spilling**: <20% performance degradation when spilling
- **Recovery**: <1 second recovery time for 1GB state
- **Compression**: >3x compression ratio for typical data

## Conclusion

This plan provides a comprehensive approach to implementing Phase 5.3's persistent storage backend, focusing on the core storage engine and memory management while deferring checkpointing to Phase 5.4. 

Key architectural decisions:
- **Pluggable serialization** through ISerializer interface for future optimization flexibility
- **ZoneTree LSM tree** adapted for DBSP's Z-set semantics and temporal ordering
- **Task-based async** throughout (not F# async) for consistency with .NET ecosystem
- **HashMap over Map** for O(1) operations in performance-critical paths
- **Adaptive memory spilling** based on GC pressure metrics

The architecture follows Feldera's key insights (shared-nothing, custom storage) while leveraging .NET ecosystem strengths (ZoneTree, MessagePack.FSharpExtensions, Task-based parallelism). The modular design allows incremental implementation and testing, with clear separation between storage (5.3) and fault tolerance (5.4) concerns.

## Status (this commit)

Current status
- Storage design aligned with Batch/Trace/Spine; composite `(K,V) -> int64` documented in `src/DBSP.Storage/STORAGE_DESIGN.md`.
- Pluggable serializers implemented (MessagePack baseline); `SerializerFactory` exposes a C#-friendly API.
- LSM storage: ZoneTree-backed with composite key + weight, iterators + range, delete-on-zero, and compaction hook; stats tracked.
- TemporalSpine: minimal in-memory implementation validates time-indexed batches and temporal queries.
- SDK/logging hygiene: `global.json` pins SDK 9.0.304; `Directory.Build.props` suppresses noisy test output.
- Tests: All storage tests are now F# only. C# `test/DBSP.Tests.Storage` removed; F# suite renamed to `DBSP.Tests.Storage` and solution updated. Tests cover serialization, LSM CRUD/range/compaction/stats, temporal queries, and basic spilling hooks. Full `dotnet test` runs clean.

Next up
- ZoneTree integration hardening: verify iterator seek/bounds and serializer Memory<byte>/byte[] conversions; remove the in-memory overlay once iterator visibility with merge/flush is validated; ensure merge/compaction preserves Z-set weight elimination.
- Temporal Spine compaction: leveled within-bucket compaction and zero elimination are implemented; extend to multi-level policies if needed; invariants tests added.
- Spilling coordination: expand `SpillCoordinator` and integrate with runtime triggers (prep for Phase 5.4), plus targeted stress tests under simulated pressure.
