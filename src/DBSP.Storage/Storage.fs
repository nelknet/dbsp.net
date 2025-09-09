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
    abstract member GetBackend<'K, 'V when 'K : comparison> : unit -> IStorageBackend<'K, 'V>
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
            task {
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
            }
            
        member _.Get(key: 'K) =
            task {
                return HashMap.tryFind key storage
            }
            
        member _.GetIterator() =
            task {
                return 
                    storage 
                    |> HashMap.toSeq
                    |> Seq.map (fun (k, (v, w)) -> (k, v, w))
            }
            
        member _.GetRangeIterator (startKey: 'K option) (endKey: 'K option) =
            task {
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
                return filtered
            }
            
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
type AdaptiveStorageManager(config: StorageConfig) =
    let monitor = 
        { new IMemoryMonitor with
            member _.GetMemoryPressure() = 
                let info = GC.GetGCMemoryInfo()
                float info.HeapSizeBytes / float info.TotalAvailableMemoryBytes
            member _.RegisterCallback(callback) = () }
    
    interface IStorageManager with
        member _.GetBackend<'K, 'V when 'K : comparison>() =
            InMemoryStorageBackend<'K, 'V>(config) :> IStorageBackend<'K, 'V>
            
        member _.GetMemoryMonitor() = monitor
        
        member _.RegisterSpillCallback(callback) = ()

    /// Public helper methods for C# tests
    member _.GetMemoryPressure() = monitor.GetMemoryPressure()
    member _.RegisterSpillCallback(callback: unit -> Task<unit>) = monitor.RegisterCallback(fun _ -> callback |> ignore)
    member _.RegisterSpillCallback(callback: System.Func<Task>) = ()

/// Hybrid storage backend with basic in-memory semantics (placeholder until disk backend stabilizes)
type HybridStorageBackend<'K, 'V when 'K : comparison>(config: StorageConfig, _serializer: ISerializer<'K * 'V>) =
    let mem = InMemoryStorageBackend<'K, 'V>(config) :> IStorageBackend<'K, 'V>
    interface IStorageBackend<'K, 'V> with
        member _.StoreBatch(updates) = mem.StoreBatch(updates)
        member _.Get(key) = mem.Get(key)
        member _.GetIterator() = mem.GetIterator()
        member _.GetRangeIterator startKey endKey = mem.GetRangeIterator startKey endKey
        member _.Compact() = mem.Compact()
        member _.GetStats() = mem.GetStats()
        member _.Dispose() = mem.Dispose()

    // C#-friendly bridging methods (ValueTuple interop)
    member this.StoreBatch(updates: struct ('K * 'V * int64) array) =
        let seq = updates |> Seq.map (fun struct (k,v,w) -> (k,v,w))
        (this :> IStorageBackend<'K,'V>).StoreBatch(seq)

    member this.Get(key: 'K) = (this :> IStorageBackend<'K,'V>).Get(key)
    member this.GetIterator() : Task<seq<struct ('K * 'V * int64)>> =
        task {
            let! it = (this :> IStorageBackend<'K,'V>).GetIterator()
            return it |> Seq.map (fun (k,v,w) -> struct (k,v,w))
        }

    member this.GetRangeIterator(startKey: int, endKey: int) : Task<seq<struct (int * 'V * int64)>> =
        task {
            let! it = (this :> IStorageBackend<'K,'V>).GetRangeIterator (Some (unbox<'K>(box startKey))) (Some (unbox<'K>(box endKey)))
            return it |> Seq.map (fun (k,v,w) -> struct (unbox<int>(box k), v, w))
        }
    // Legacy-style overloads (for F# callers)
    member this.GetIteratorLegacy() = (this :> IStorageBackend<'K,'V>).GetIterator()
    member this.GetRangeIteratorLegacy(startKey: 'K, endKey: 'K) = (this :> IStorageBackend<'K,'V>).GetRangeIterator (Some startKey) (Some endKey)


// Hybrid storage backend intentionally omitted until on-disk backend stabilizes

// Temporal trace/spine lives in TemporalStorage.fs (not included yet)
