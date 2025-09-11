namespace DBSP.Storage

open System
open System.Threading.Tasks
open FSharp.Data.Adaptive

/// Configuration for storage backends
[<CLIMutable>]
type StorageConfig = {
    DataPath: string
    MaxMemoryBytes: int64
    CompactionThreshold: int
    WriteBufferSize: int
    BlockCacheSize: int64
    SpillThreshold: float
}

/// Statistics for storage operations
type StorageStats = {
    BytesWritten: int64
    BytesRead: int64
    KeysStored: int64
    CompactionCount: int
    LastCompactionTime: DateTime option
}

// Serialization interfaces and factories are defined in Serialization.fs

/// Storage backend interface for Z-sets
type IStorageBackend<'K, 'V when 'K : comparison> =
    /// Store a batch of updates
    abstract member StoreBatch: updates: ('K * 'V * int64) seq -> Task<unit>
    
    /// Retrieve value and weight for a key
    abstract member Get: key: 'K -> Task<('V * int64) option>
    
    /// Iterate over all stored entries
    abstract member GetIterator: unit -> Task<seq<'K * 'V * int64>>
    
    /// Iterate over a key range
    abstract member GetRangeIterator: startKey: 'K option -> endKey: 'K option -> Task<seq<'K * 'V * int64>>
    
    /// Compact storage
    abstract member Compact: unit -> Task<unit>
    
    /// Get storage statistics
    abstract member GetStats: unit -> Task<StorageStats>
    
    /// Dispose resources
    abstract member Dispose: unit -> Task<unit>

/// Memory pressure monitor
type IMemoryMonitor =
    abstract member GetMemoryPressure: unit -> float
    abstract member RegisterCallback: callback: (float -> unit) -> unit

/// Storage manager interface
type IStorageManager =
    abstract member GetBackend<'K, 'V when 'K : comparison and 'V : comparison> : unit -> IStorageBackend<'K, 'V>
    abstract member GetMemoryMonitor: unit -> IMemoryMonitor
    abstract member RegisterSpillCallback: callback: (unit -> Task<unit>) -> unit

// SerializerFactory provided by Serialization.fs

/// In-memory storage backend implementation using HashMap
type InMemoryStorageBackend<'K, 'V when 'K : comparison>(config: StorageConfig) =
    let mutable storage = HashMap.empty<'K, 'V * int64>
    let mutable stats = {
        BytesWritten = 0L
        BytesRead = 0L
        KeysStored = 0L
        CompactionCount = 0
        LastCompactionTime = None
    }
    
    interface IStorageBackend<'K, 'V> with
        member _.StoreBatch(updates: ('K * 'V * int64) seq) =
            // Synchronous implementation; return completed task to avoid FS3511 warnings
            for (key, value, weight) in updates do
                storage <-
                    match HashMap.tryFind key storage with
                    | Some (_, existingWeight) ->
                        let newWeight = existingWeight + weight
                        if newWeight = 0L then HashMap.remove key storage
                        else HashMap.add key (value, newWeight) storage
                    | None ->
                        if weight <> 0L then
                            HashMap.add key (value, weight) storage
                        else
                            storage
            stats <- 
                { stats with 
                    BytesWritten = stats.BytesWritten + 100L // Simplified
                    KeysStored = int64 (HashMap.count storage) }
            Task.FromResult(())
            
        member _.Get(key: 'K) =
            // Pure lookup; return cached value via Task.FromResult
            Task.FromResult(HashMap.tryFind key storage)
            
        member _.GetIterator() =
            // Wrap synchronous iterator in Task
            Task.FromResult(
                storage 
                |> HashMap.toSeq
                |> Seq.map (fun (k, (v, w)) -> (k, v, w))
            )
            
        member _.GetRangeIterator (startKey: 'K option) (endKey: 'K option) =
            // Filter keys synchronously and wrap in Task
            let filtered = 
                storage 
                |> HashMap.toSeq
                |> Seq.filter (fun (k, _) ->
                    let afterStart = 
                        match startKey with
                        | Some s -> k >= s
                        | None -> true
                    let beforeEnd =
                        match endKey with
                        | Some e -> k <= e
                        | None -> true
                    afterStart && beforeEnd)
                |> Seq.map (fun (k, (v, w)) -> (k, v, w))
            Task.FromResult(filtered)
            
        member _.Compact() =
            task {
                // In-memory backend doesn't need compaction
                stats <- 
                    { stats with 
                        CompactionCount = stats.CompactionCount + 1
                        LastCompactionTime = Some DateTime.UtcNow }
            }
            
        member _.GetStats() =
            task {
                return stats
            }
            
        member _.Dispose() =
            task {
                storage <- HashMap.empty
            }

// LSMStorageBackend is implemented in LSMStorage.fs

/// Adaptive storage manager
// Moved AdaptiveStorageManager and HybridStorageBackend to Storage.Backends.fs for compile order.

/// Hybrid storage backend with basic in-memory semantics (placeholder until disk backend stabilizes)
// HybridStorageBackend moved to Storage.Backends.fs


// Hybrid storage backend intentionally omitted until on-disk backend stabilizes

// Temporal trace/spine lives in TemporalStorage.fs (not included yet)

/// Time-aware trace interface for persistent Z-set storage.
type ITemporalTrace<'K,'V when 'K : comparison and 'V : comparison> =
    /// Insert a batch of updates at logical time.
    abstract member InsertBatch: time:int64 * updates: seq<'K * 'V * int64> -> Task<unit>
    /// Snapshot all updates with T <= time.
    abstract member QueryAtTime: time:int64 -> Task<seq<struct('K*'V*int64)>>
    /// Enumerate batches within a time range [start,end].
    abstract member QueryTimeRange: startTime:int64 * endTime:int64 -> Task<seq<struct(int64 * struct('K*'V*int64) array)>>
    /// Maintenance: compact older times into coarser buckets.
    abstract member Maintain: beforeTime:int64 * bucketSize:int64 -> Task<unit>
