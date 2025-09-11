namespace DBSP.Storage

open System
open System.IO
open System.Threading.Tasks
open FSharp.Data.Adaptive
open System.Collections.Generic
open Tenray.ZoneTree
open Tenray.ZoneTree.Options
open Tenray.ZoneTree.Comparers
open Tenray.ZoneTree.Serializers

/// Composite key of (K, V) for Z-set entries; value is weight (int64)
type KV<'K,'V when 'K : comparison and 'V : comparison> =
    { K: 'K; V: 'V }

/// ZoneTree-backed LSM storage using composite (K,V) -> weight
type LSMStorageBackend<'K, 'V when 'K : comparison and 'V : comparison>
    (config: StorageConfig, keyTupleSerializer: DBSP.Storage.ISerializer<System.ValueTuple<'K,'V>>) =

    // Comparer for KV: compare by K then V
    let kvComparer =
        { new IRefComparer<KV<'K,'V>> with
            member _.Compare(x: inref<KV<'K,'V>>, y: inref<KV<'K,'V>>) =
                let c1 = compare x.K y.K
                if c1 <> 0 then (if c1 < 0 then -1 else 1)
                else
                    let c2 = compare x.V y.V
                    if c2 < 0 then -1 elif c2 > 0 then 1 else 0 }

    // Tenray serializers bridging to our pluggable serializer
    let keySerializer : ISerializer<KV<'K,'V>> =
        { new ISerializer<KV<'K,'V>> with
            member _.Serialize(entry: inref<KV<'K,'V>>) =
                let bytes = keyTupleSerializer.Serialize(struct (entry.K, entry.V))
                new Memory<byte>(bytes)
            member _.Deserialize(bytes: Memory<byte>) =
                let arr = bytes.ToArray()
                let struct (k,v) = keyTupleSerializer.Deserialize(arr)
                { K = k; V = v } }

    let valueSerializer : ISerializer<int64> =
        { new ISerializer<int64> with
            member _.Serialize(value: inref<int64>) =
                let b = BitConverter.GetBytes(value)
                new Memory<byte>(b)
            member _.Deserialize(bytes: Memory<byte>) =
                let span = bytes.Span
                if span.Length >= 8 then BitConverter.ToInt64(span.Slice(0,8)) else 0L }

    // Build or open the ZoneTree database
    let zoneTree : IZoneTree<KV<'K,'V>, int64> =
        let factory = new ZoneTreeFactory<KV<'K,'V>, int64>()
        let dataDir = Path.Combine(config.DataPath, "lsm")
        Directory.CreateDirectory(dataDir) |> ignore
        factory
            .SetDataDirectory(dataDir)
            .SetComparer(kvComparer)
            .SetKeySerializer(keySerializer)
            .SetValueSerializer(valueSerializer)
            .SetDiskSegmentMaxItemCount(config.CompactionThreshold)
            .ConfigureDiskSegmentOptions(fun o ->
                // Tune key/value cache sizes heuristically based on provided BlockCacheSize
                if config.BlockCacheSize > 0L then
                    let cache = max 1024 (int (config.BlockCacheSize / 64L))
                    o.KeyCacheSize <- cache
                    o.ValueCacheSize <- cache
            )
            .OpenOrCreate()

    // ZoneTree is the durable store; reads use ZoneTree iterators with lower-bound seek on composite key.

    let mutable stats = {
        BytesWritten = 0L
        BytesRead = 0L
        KeysStored = 0L
        CompactionCount = 0
        LastCompactionTime = None
    }

    interface IStorageBackend<'K, 'V> with
        member _.StoreBatch(updates: ('K * 'V * int64) seq) =
            // Synchronous implementation; wrap with Task.FromResult to avoid FS3511
            // Optimization: coalesce updates per (K,V) within the batch to reduce write amplification.
            let aggregated = Dictionary<KV<'K,'V>, int64>()
            for (k, v, w) in updates do
                if w <> 0L then
                    let key = { K = k; V = v }
                    match aggregated.TryGetValue(key) with
                    | true, existing -> aggregated[key] <- existing + w
                    | _ -> aggregated[key] <- w
            // Apply coalesced deltas against the tree
            for kvp in aggregated do
                let kv = kvp.Key
                let delta = kvp.Value
                if delta <> 0L then
                    let mutable existing = 0L
                    let mutable keyCopy = kv
                    if zoneTree.TryGet(&keyCopy, &existing) then
                        let newW = existing + delta
                        if newW = 0L then
                            let mutable _op = 0L
                            zoneTree.TryDelete(&keyCopy, & _op) |> ignore
                        else
                            zoneTree.Upsert(&keyCopy, &newW) |> ignore
                    else
                        // No prior value; insert delta if non-zero
                        zoneTree.Upsert(&keyCopy, &delta) |> ignore
                    stats <- { stats with BytesWritten = stats.BytesWritten + 16L }
            stats <- { stats with KeysStored = int64 (zoneTree.Count()) }
            Task.FromResult(())

        member _.Get(k: 'K) =
            // Seek to the first key >= (k, default V) and scan only matching K
            use it = zoneTree.CreateIterator()
            let mutable lb = { K = k; V = Unchecked.defaultof<'V> }
            it.Seek(&lb) |> ignore
            stats <- { stats with BytesRead = stats.BytesRead + 16L }
            let mutable res : ('V * int64) option = None
            let mutable cont = it.Next()
            while cont && res.IsNone do
                let ck = it.CurrentKey
                if ck.K = k then res <- Some (ck.V, it.CurrentValue)
                elif ck.K > k then cont <- false
                else cont <- it.Next()
            Task.FromResult(res)

        member _.GetIterator() =
            let it = zoneTree.CreateIterator()
            let seq = seq {
                while it.Next() do
                    let k = it.CurrentKey
                    let w = it.CurrentValue
                    if w <> 0L then yield (k.K, k.V, w)
                it.Dispose()
            }
            Task.FromResult(seq)

        member _.GetRangeIterator (startKey: 'K option) (endKey: 'K option) =
            let it = zoneTree.CreateIterator()
            // Without a generic minimal V, we scan forward until K >= startKey
            let startOk = it.Next()
            let seq = seq {
                if startOk then
                    let mutable cont = true
                    while cont do
                        let ck = it.CurrentKey
                        let within =
                            (match startKey with Some s -> ck.K >= s | None -> true) &&
                            (match endKey with Some e -> ck.K <= e | None -> true)
                        if within then
                            let w = it.CurrentValue
                            if w <> 0L then yield (ck.K, ck.V, w)
                            cont <- it.Next()
                        else if (match endKey with Some e when ck.K > e -> true | _ -> false) then
                            cont <- false
                        else
                            cont <- it.Next()
                it.Dispose()
            }
            Task.FromResult(seq)

        member _.Compact() =
            // Force memtable forward then merge until no in-memory records remain
            let m = zoneTree.Maintenance
            let mutable remaining = m.InMemoryRecordCount
            while remaining > 0L do
                m.MoveMutableSegmentForward()
                m.StartMergeOperation().Join() |> ignore
                remaining <- m.InMemoryRecordCount
            stats <- { stats with CompactionCount = stats.CompactionCount + 1; LastCompactionTime = Some DateTime.UtcNow }
            Task.FromResult(())

        member _.GetStats() = Task.FromResult(stats)
        member _.Dispose() =
            zoneTree.Dispose()
            Task.FromResult(())

    // C#-friendly overloads
    member this.StoreBatch(updates: struct ('K * 'V * int64) array) =
        let seq = updates |> Seq.map (fun struct (k,v,w) -> (k,v,w))
        (this :> IStorageBackend<'K,'V>).StoreBatch(seq)

    /// Extended API: optionally force synchronous flush/merge for immediate visibility (tests/benchmarks)
    member this.StoreBatchWithFlush(updates: ('K * 'V * int64) seq, syncFlush: bool) =
        task {
            do! (this :> IStorageBackend<'K,'V>).StoreBatch(updates)
            if syncFlush then
                let m = zoneTree.Maintenance
                let mutable remaining = m.InMemoryRecordCount
                while remaining > 0L do
                    m.MoveMutableSegmentForward()
                    m.StartMergeOperation().Join() |> ignore
                    remaining <- m.InMemoryRecordCount
        }

    member this.StoreBatch(updates: struct ('K * 'V * int64) array, syncFlush: bool) =
        task {
            do! this.StoreBatch(updates)
            if syncFlush then
                let m = zoneTree.Maintenance
                let mutable remaining = m.InMemoryRecordCount
                while remaining > 0L do
                    m.MoveMutableSegmentForward()
                    m.StartMergeOperation().Join() |> ignore
                    remaining <- m.InMemoryRecordCount
        }

    member this.GetIterator() : Task<seq<struct ('K * 'V * int64)>> =
        task {
            let! it = (this :> IStorageBackend<'K,'V>).GetIterator()
            return it |> Seq.map (fun (k,v,w) -> struct (k,v,w))
        }

    // Compact and stats passthroughs
    member this.Compact() = (this :> IStorageBackend<'K,'V>).Compact()
    member this.GetStats() = (this :> IStorageBackend<'K,'V>).GetStats()
