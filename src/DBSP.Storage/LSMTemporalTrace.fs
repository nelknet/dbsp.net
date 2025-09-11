namespace DBSP.Storage

open System
open System.Collections.Generic
open System.Threading.Tasks
open Tenray.ZoneTree
open Tenray.ZoneTree.Options
open Tenray.ZoneTree.Comparers
open Tenray.ZoneTree.Serializers

/// Composite key of (time, K, V) for temporal Z-set entries.
type TKV<'K,'V when 'K : comparison and 'V : comparison> =
    { T: int64; K: 'K; V: 'V }

/// Temporal trace stored over ZoneTree with composite (T,K,V) -> weight.
/// Provides batch-at-time ingestion and snapshot/time-range queries.
type LSMTemporalTrace<'K,'V when 'K : comparison and 'V : comparison>
    (config: StorageConfig,
     tupleSerializer: DBSP.Storage.ISerializer<System.ValueTuple<int64,'K,'V>>) =

    // Comparer for TKV: order by time, then K, then V.
    let tkvComparer =
        { new IRefComparer<TKV<'K,'V>> with
            member _.Compare(x: inref<TKV<'K,'V>>, y: inref<TKV<'K,'V>>) =
                if x.T < y.T then -1 elif x.T > y.T then 1
                else
                    let c1 = compare x.K y.K
                    if c1 <> 0 then (if c1 < 0 then -1 else 1)
                    else
                        let c2 = compare x.V y.V
                        if c2 < 0 then -1 elif c2 > 0 then 1 else 0 }

    // Bridge to tuple serializer for (T,K,V)
    let keySerializer : ISerializer<TKV<'K,'V>> =
        { new ISerializer<TKV<'K,'V>> with
            member _.Serialize(entry: inref<TKV<'K,'V>>) =
                let bytes = tupleSerializer.Serialize(struct (entry.T, entry.K, entry.V))
                new Memory<byte>(bytes)
            member _.Deserialize(bytes: Memory<byte>) =
                let arr = bytes.ToArray()
                let struct (t,k,v) = tupleSerializer.Deserialize(arr)
                { T = t; K = k; V = v } }

    let valueSerializer : ISerializer<int64> =
        { new ISerializer<int64> with
            member _.Serialize(value: inref<int64>) = new Memory<byte>(BitConverter.GetBytes(value))
            member _.Deserialize(bytes: Memory<byte>) =
                let span = bytes.Span
                if span.Length >= 8 then BitConverter.ToInt64(span.Slice(0,8)) else 0L }

    // Open temporal ZoneTree at DataPath/lsm_temporal
    let zoneTree : IZoneTree<TKV<'K,'V>, int64> =
        let factory = new ZoneTreeFactory<TKV<'K,'V>, int64>()
        let dataDir = System.IO.Path.Combine(config.DataPath, "lsm_temporal")
        System.IO.Directory.CreateDirectory(dataDir) |> ignore
        factory
            .SetDataDirectory(dataDir)
            .SetComparer(tkvComparer)
            .SetKeySerializer(keySerializer)
            .SetValueSerializer(valueSerializer)
            .SetDiskSegmentMaxItemCount(config.CompactionThreshold)
            .OpenOrCreate()

    /// Insert a batch of updates at a logical time. Coalesces within-batch duplicates.
    member _.InsertBatch(time: int64, updates: seq<'K * 'V * int64>) =
        // Aggregate by (K,V) to reduce write amplification
        let agg = Dictionary<struct('K*'V), int64>()
        for (k,v,w) in updates do
            if w <> 0L then
                let key = struct(k,v)
                match agg.TryGetValue(key) with
                | true, existing -> agg[key] <- existing + w
                | _ -> agg[key] <- w
        for kvp in agg do
            let struct(k,v) = kvp.Key
            let delta = kvp.Value
            if delta <> 0L then
                let mutable key = { T = time; K = k; V = v }
                let mutable existing = 0L
                if zoneTree.TryGet(&key, &existing) then
                    let newW = existing + delta
                    if newW = 0L then
                        let mutable _op = 0L
                        zoneTree.TryDelete(&key, & _op) |> ignore
                    else
                        zoneTree.Upsert(&key, &newW) |> ignore
                else
                    zoneTree.Upsert(&key, &delta) |> ignore
        Task.FromResult(())

    /// Snapshot at time: merges all entries with T <= time, summing weights per (K,V).
    member _.QueryAtTime(time: int64) : Task<seq<struct('K*'V*int64)>> =
        task {
            use it = zoneTree.CreateIterator()
            let dict = Dictionary<struct('K*'V), int64>()
            while it.Next() do
                let k = it.CurrentKey
                if k.T <= time then
                    let key = struct(k.K, k.V)
                    let w = it.CurrentValue
                    match dict.TryGetValue(key) with
                    | true, existing -> dict[key] <- existing + w
                    | _ -> dict[key] <- w
            let seq =
                dict
                |> Seq.choose (fun (KeyValue(struct(k,v),w)) -> if w = 0L then None else Some (struct(k,v,w)))
            return seq
        }

    /// Enumerate batches within [startTime, endTime], returning per-time arrays.
    member _.QueryTimeRange(startTime: int64, endTime: int64) : Task<seq<struct(int64 * struct('K*'V*int64) array)>> =
        task {
            use it = zoneTree.CreateIterator()
            let current = ResizeArray<struct(int64 * struct('K*'V*int64) array)>()
            let acc = Dictionary<int64, ResizeArray<struct('K*'V*int64)>>()
            while it.Next() do
                let k = it.CurrentKey
                if k.T >= startTime && k.T <= endTime then
                    let w = it.CurrentValue
                    if w <> 0L then
                        let bucket =
                            match acc.TryGetValue(k.T) with
                            | true, b -> b
                            | _ ->
                                let b = ResizeArray()
                                acc[k.T] <- b
                                b
                        bucket.Add(struct(k.K, k.V, w))
            for kv in acc do
                current.Add(struct(kv.Key, kv.Value.ToArray()))
            // Sort by time
            let arr = current.ToArray()
            Array.sortInPlaceBy (fun struct(t,_) -> t) arr
            return arr :> seq<_>
        }

    /// Compact all entries with T < beforeTime by coalescing duplicates; routine maintenance.
    member _.Compact(beforeTime: int64) =
        task {
            use it = zoneTree.CreateIterator()
            let toRewrite = Dictionary<TKV<'K,'V>, int64>()
            while it.Next() do
                let k = it.CurrentKey
                if k.T < beforeTime then
                    // Already maintained as single aggregated entry per (T,K,V); nothing to merge within same key.
                    // This path exists to future-proof per-time coalescing if multiple entries per key appear.
                    toRewrite[k] <- it.CurrentValue
            // No-op for now; ZoneTree already holds a single value per (T,K,V)
            toRewrite.Clear()
            return ()
        }

    interface IDisposable with
        member _.Dispose() = zoneTree.Dispose()
