namespace DBSP.Storage

open System
open System.IO
open System.Threading.Tasks

/// Hybrid storage backend now delegates directly to the LSM backend (overlay removed)
type HybridStorageBackend<'K, 'V when 'K : comparison and 'V : comparison>(config: StorageConfig, serializer: ISerializer<'K * 'V>) =
    // Bridge F# tuple serializer to ValueTuple serializer expected by LSM layer
    let toValueTupleSerializer (s: ISerializer<'K * 'V>) : ISerializer<System.ValueTuple<'K,'V>> =
        { new ISerializer<System.ValueTuple<'K,'V>> with
            member _.Serialize(value: System.ValueTuple<'K,'V>) =
                let struct (k,v) = value
                s.Serialize((k, v))
            member _.Deserialize(bytes: byte[]) =
                let (k,v) = s.Deserialize(bytes)
                struct (k, v)
            member _.EstimateSize(value: System.ValueTuple<'K,'V>) =
                let struct (k,v) = value
                s.EstimateSize((k, v)) }

    // Use a unique subdirectory per instance to avoid test collisions when DataPath points at a shared temp dir
    let uniquePath = Path.Combine(config.DataPath, "dbsp_hybrid_" + string (Guid.NewGuid()))
    let cfg = { config with DataPath = uniquePath }
    let disk = LSMStorageBackend<'K,'V>(cfg, toValueTupleSerializer serializer) :> IStorageBackend<'K,'V>

    interface IStorageBackend<'K, 'V> with
        member _.StoreBatch(updates) = disk.StoreBatch(updates)
        member _.Get(key) = disk.Get(key)
        member _.GetIterator() = disk.GetIterator()
        member _.GetRangeIterator startKey endKey = disk.GetRangeIterator startKey endKey
        member _.Compact() = disk.Compact()
        member _.GetStats() = disk.GetStats()
        member _.Dispose() = disk.Dispose()

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
