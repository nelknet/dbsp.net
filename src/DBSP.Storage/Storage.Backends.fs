namespace DBSP.Storage

open System.Threading.Tasks
open FSharp.Data.Adaptive

type StorageMode = InMemory | LSM | Hybrid

/// Simple hybrid backend: in-memory buffer with thresholded spill to LSM
type HybridStorageBackend<'K, 'V when 'K : comparison and 'V : comparison>(config: StorageConfig, _serializer: ISerializer<'K * 'V>) =
    let mutable mem = HashMap.empty<'K, 'V * int64>
    let disk =
        let ser = SerializerFactory.CreateMessagePack<struct('K * 'V)>()
        let uniquePath = System.IO.Path.Combine(config.DataPath, "hybrid_" + string (System.Guid.NewGuid()))
        let cfg = { config with DataPath = uniquePath }
        LSMStorageBackend<'K,'V>(cfg, ser) :> IStorageBackend<'K,'V>

    let approxItemSize = 64L
    let spillThresholdBytes = int64 (float config.MaxMemoryBytes * config.SpillThreshold)
    let spillThresholdItems = max config.CompactionThreshold 1024

    let shouldSpill() =
        let items = HashMap.count mem
        let bytes = int64 items * approxItemSize
        items >= spillThresholdItems || bytes >= spillThresholdBytes

    let flushToDisk() = task {
        if not (HashMap.isEmpty mem) then
            let batch = mem |> HashMap.toSeq |> Seq.map (fun (k,(v,w)) -> (k,v,w))
            do! disk.StoreBatch(batch)
            mem <- HashMap.empty
        }

    interface IStorageBackend<'K, 'V> with
        member _.StoreBatch(updates) =
            task {
                for (k,v,w) in updates do
                    let updated =
                        match HashMap.tryFind k mem with
                        | Some (v0,w0) -> let w' = w0 + w in if w' = 0L then None else Some (v0, w')
                        | None -> if w = 0L then None else Some (v, w)
                    mem <- match updated with None -> HashMap.remove k mem | Some t -> HashMap.add k t mem
                if shouldSpill() then do! flushToDisk()
            }
        member _.Get(key) =
            match HashMap.tryFind key mem with
            | Some (v,w) -> Task.FromResult(Some (v,w))
            | None -> disk.Get(key)
        member _.GetIterator() =
            task {
                let! dit = disk.GetIterator()
                let d = dit |> Seq.toArray
                let md = mem |> HashMap.toSeq |> Seq.map (fun (k,(v,w)) -> (k,v,w)) |> Seq.toArray
                let dict = System.Collections.Generic.Dictionary<'K, 'V * int64>()
                for (k,v,w) in d do dict[k] <- (v,w)
                for (k,v,w) in md do dict[k] <- (v,w)
                let seq = dict |> Seq.map (fun kv -> let (k,(v,w)) = kv.Key, kv.Value in (k,v,w))
                return seq
            }
        member _.GetRangeIterator (startKey: 'K option) (endKey: 'K option) =
            task {
                let! dit = disk.GetRangeIterator startKey endKey
                let md =
                    mem
                    |> HashMap.toSeq
                    |> Seq.filter (fun (k,_) ->
                        (match startKey with Some s -> k >= s | None -> true) &&
                        (match endKey with Some e -> k <= e | None -> true))
                    |> Seq.map (fun (k,(v,w)) -> (k,v,w))
                let d = dit |> Seq.toArray
                let dict = System.Collections.Generic.Dictionary<'K, 'V * int64>()
                for (k,v,w) in d do dict[k] <- (v,w)
                for (k,v,w) in md do dict[k] <- (v,w)
                let seq = dict |> Seq.map (fun kv -> let (k,(v,w)) = kv.Key, kv.Value in (k,v,w))
                return seq
            }
        member _.Compact() =
            task {
                do! flushToDisk()
                do! disk.Compact()
            }
        member _.GetStats() = disk.GetStats()
        member _.Dispose() = disk.Dispose()

/// Adaptive storage manager choosing backend by mode (default LSM)
type AdaptiveStorageManager(config: StorageConfig, ?mode: StorageMode) =
    let monitor = 
        { new IMemoryMonitor with
            member _.GetMemoryPressure() = 
                let info = System.GC.GetGCMemoryInfo()
                float info.HeapSizeBytes / float info.TotalAvailableMemoryBytes
            member _.RegisterCallback(_callback) = () }
    let mutable mode' = defaultArg mode StorageMode.LSM

    interface IStorageManager with
        member _.GetBackend<'K, 'V when 'K : comparison and 'V : comparison>() =
            match mode' with
            | InMemory -> InMemoryStorageBackend<'K, 'V>(config) :> IStorageBackend<'K, 'V>
            | LSM ->
                let ser = SerializerFactory.CreateMessagePack<struct('K * 'V)>()
                LSMStorageBackend<'K,'V>(config, ser) :> IStorageBackend<'K,'V>
            | Hybrid ->
                let serRef = SerializerFactory.CreateMessagePack<'K * 'V>()
                HybridStorageBackend<'K,'V>(config, serRef) :> IStorageBackend<'K,'V>
        member _.GetMemoryMonitor() = monitor
        member _.RegisterSpillCallback(_callback) = ()

    member _.GetMemoryPressure() = monitor.GetMemoryPressure()
    member _.RegisterSpillCallback(callback: unit -> Task<unit>) = monitor.RegisterCallback(fun _ -> callback |> ignore)
    member _.RegisterSpillCallback(_callback: System.Func<Task>) = ()
    member _.SetMode(m: StorageMode) = mode' <- m
