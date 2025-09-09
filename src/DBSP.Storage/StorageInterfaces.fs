namespace DBSP.Storage

open System
open System.Threading.Tasks
open DBSP.Core.Algebra
open FSharp.Data.Adaptive

/// Configuration for storage backends
type StorageConfig = {
    DataPath: string
    MaxMemoryBytes: int64
    CompactionThreshold: int
    WriteBufferSize: int
    BlockCacheSize: int64
}

/// Statistics for storage operations
type StorageStats = {
    BytesWritten: int64
    BytesRead: int64
    KeysStored: int64
    CompactionCount: int
    LastCompactionTime: DateTime option
}

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
    abstract member GetUsedMemoryBytes: unit -> int64
    abstract member GetAvailableMemoryBytes: unit -> int64
    abstract member RegisterPressureCallback: callback: (float -> unit) -> unit
    abstract member UnregisterPressureCallback: callback: (float -> unit) -> unit

/// Spilling policy for memory management
type SpillingPolicy =
    | Adaptive of thresholdPercent: float
    | Fixed of maxMemoryBytes: int64
    | Manual

/// Storage layer manager
type IStorageManager<'K, 'V when 'K : comparison> =
    /// Create a new storage backend
    abstract member CreateBackend: name: string -> Task<IStorageBackend<'K, 'V>>
    
    /// Get or create a storage backend
    abstract member GetBackend: name: string -> Task<IStorageBackend<'K, 'V>>
    
    /// Delete a storage backend
    abstract member DeleteBackend: name: string -> Task<unit>
    
    /// List all backends
    abstract member ListBackends: unit -> Task<string list>
    
    /// Set spilling policy
    abstract member SetSpillingPolicy: policy: SpillingPolicy -> unit
    
    /// Force spill to disk
    abstract member ForceSpill: backendName: string -> Task<unit>
    
    /// Dispose all resources
    abstract member Dispose: unit -> Task<unit>