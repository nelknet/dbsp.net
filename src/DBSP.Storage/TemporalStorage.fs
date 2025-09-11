namespace DBSP.Storage

open System
open System.Collections.Generic
open System.Threading.Tasks

/// Minimal temporal spine that tracks batches by time externally (no time in keys)
type TemporalSpine<'K, 'V when 'K : comparison and 'V : equality>(config: StorageConfig, _serializer: ISerializer<struct ('K * 'V)>) =
    // Store per-time snapshots as simple lists; use in-memory backend for data
    let mutable frontier = 0L
    let snapshots = SortedDictionary<int64, ResizeArray<'K * 'V * int64>>()

    member _.InsertBatch(time: int64, updates: struct ('K * 'V * int64) array) =
        if not (snapshots.ContainsKey(time)) then snapshots[time] <- ResizeArray()
        snapshots[time].AddRange(updates |> Seq.map (fun struct (k,v,w) -> (k,v,w)))
        Task.FromResult(())


    member _.QueryAtTime(time: int64) =
        let acc = Dictionary<'K * 'V, int64>()
        for kv in snapshots do
            if kv.Key <= time then
                for (k,v,w) in kv.Value do
                    let key = (k,v)
                    match acc.TryGetValue(key) with
                    | true, w0 -> acc[key] <- w0 + w
                    | _ -> acc[key] <- w
        let res =
            acc
            |> Seq.choose (fun (KeyValue((k,v),w)) -> if w = 0L then None else Some (struct (k,v,w)))
            |> Seq.toArray
        Task.FromResult(res)


    member _.QueryTimeRange(startTime: int64, endTime: int64) =
        let res =
            snapshots
            |> Seq.filter (fun kv -> kv.Key >= startTime && kv.Key <= endTime)
            |> Seq.map (fun kv -> struct (kv.Key, kv.Value |> Seq.map (fun (k,v,w) -> struct (k,v,w)) |> Seq.toArray))
            |> Seq.toArray
        Task.FromResult(res)


    member _.Compact(beforeTime: int64) =
        // Leveled compaction: within-bucket merge for all times < beforeTime.
        // This eliminates duplicate keys within the same time and removes zero-weight pairs,
        // while preserving per-time queryability.
        let toProcess = snapshots.Keys |> Seq.filter (fun t -> t < beforeTime) |> Seq.toArray
        for t in toProcess do
            let bucket = snapshots[t]
            let acc = Dictionary<'K * 'V, int64>()
            for (k,v,w) in bucket do
                let key = (k,v)
                match acc.TryGetValue(key) with
                | true, w0 -> acc[key] <- w0 + w
                | _ -> acc[key] <- w
            // Rewrite bucket with filtered entries
            let merged = ResizeArray<'K * 'V * int64>()
            for kv in acc do
                if kv.Value <> 0L then
                    let (k,v) = kv.Key
                    merged.Add(k, v, kv.Value)
            snapshots[t] <- merged
        Task.FromResult(())

    member _.AdvanceFrontier(newFrontier: int64) =
        frontier <- max frontier newFrontier
        Task.FromResult(())
