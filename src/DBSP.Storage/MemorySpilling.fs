namespace DBSP.Storage

open System
open System.Threading.Tasks
open System.Collections.Concurrent
open FSharp.Data.Adaptive

/// Memory monitor implementation
type MemoryMonitor() =
    let callbacks = ConcurrentBag<float -> unit>()
    
    let mutable lastPressureLevel = 0.0
    
    // Background task to monitor memory
    let monitorTask = 
        Task.Run(fun () ->
            task {
                while true do
                    let gcInfo = GC.GetMemoryInfo()
                    let usedBytes = GC.GetTotalMemory(false)
                    let totalBytes = gcInfo.TotalAvailableMemoryBytes
                    let pressure = float usedBytes / float totalBytes
                    
                    if abs(pressure - lastPressureLevel) > 0.05 then
                        lastPressureLevel <- pressure
                        for callback in callbacks do
                            callback pressure
                    
                    do! Task.Delay(1000)
            }
        )
    
    interface IMemoryMonitor with
        member _.GetUsedMemoryBytes() =
            GC.GetTotalMemory(false)
        
        member _.GetAvailableMemoryBytes() =
            let gcInfo = GC.GetMemoryInfo()
            gcInfo.TotalAvailableMemoryBytes - GC.GetTotalMemory(false)
        
        member _.RegisterPressureCallback(callback) =
            callbacks.Add(callback)
        
        member _.UnregisterPressureCallback(callback) =
            callbacks.TryTake() |> ignore

/// Adaptive spilling storage manager
type AdaptiveStorageManager<'K, 'V when 'K : comparison>(
    config: StorageConfig,
    serializer: ISerializer<'K * 'V>) =
    
    let backends = ConcurrentDictionary<string, IStorageBackend<'K, 'V>>()
    let memoryMonitor = MemoryMonitor() :> IMemoryMonitor
    let mutable spillingPolicy = SpillingPolicy.Adaptive 0.8
    
    // Track in-memory data for spilling decisions
    let inMemoryData = ConcurrentDictionary<string, HashMap<'K, 'V * int64>>()
    
    // Spill data when memory pressure is high
    let spillToDisk (backendName: string) =
        task {
            match inMemoryData.TryGetValue(backendName) with
            | true, data when not data.IsEmpty ->
                match backends.TryGetValue(backendName) with
                | true, backend ->
                    // Convert HashMap to sequence and spill
                    let updates = 
                        data 
                        |> HashMap.toSeq
                        |> Seq.map (fun (k, (v, w)) -> (k, v, w))
                    
                    do! backend.StoreBatch(updates)
                    inMemoryData.TryUpdate(backendName, HashMap.empty, data) |> ignore
                | false, _ -> ()
            | _ -> ()
        }
    
    // Register memory pressure callback
    do
        memoryMonitor.RegisterPressureCallback(fun pressure ->
            match spillingPolicy with
            | Adaptive threshold when pressure > threshold ->
                // Spill all in-memory data
                for kvp in inMemoryData do
                    spillToDisk kvp.Key |> ignore
            | Fixed maxBytes ->
                let used = memoryMonitor.GetUsedMemoryBytes()
                if used > maxBytes then
                    for kvp in inMemoryData do
                        spillToDisk kvp.Key |> ignore
            | _ -> ()
        )
    
    interface IStorageManager<'K, 'V> with
        member _.CreateBackend(name: string) =
            task {
                let backend = LSMStorageBackend<'K, 'V>(config, serializer) :> IStorageBackend<'K, 'V>
                backends.TryAdd(name, backend) |> ignore
                inMemoryData.TryAdd(name, HashMap.empty) |> ignore
                return backend
            }
        
        member _.GetBackend(name: string) =
            task {
                match backends.TryGetValue(name) with
                | true, backend -> return backend
                | false, _ -> 
                    let backend = LSMStorageBackend<'K, 'V>(config, serializer) :> IStorageBackend<'K, 'V>
                    backends.TryAdd(name, backend) |> ignore
                    inMemoryData.TryAdd(name, HashMap.empty) |> ignore
                    return backend
            }
        
        member _.DeleteBackend(name: string) =
            task {
                match backends.TryRemove(name) with
                | true, backend -> 
                    do! backend.Dispose()
                    inMemoryData.TryRemove(name) |> ignore
                | false, _ -> ()
            }
        
        member _.ListBackends() =
            task {
                return backends.Keys |> Seq.toList
            }
        
        member _.SetSpillingPolicy(policy: SpillingPolicy) =
            spillingPolicy <- policy
        
        member _.ForceSpill(backendName: string) =
            spillToDisk backendName
        
        member _.Dispose() =
            task {
                for kvp in backends do
                    do! kvp.Value.Dispose()
                backends.Clear()
                inMemoryData.Clear()
            }

/// Hybrid storage backend that combines in-memory and disk storage
type HybridStorageBackend<'K, 'V when 'K : comparison>(
    memoryBackend: HashMap<'K, 'V * int64>,
    diskBackend: IStorageBackend<'K, 'V>,
    spillThreshold: int) =
    
    let mutable memory = memoryBackend
    let mutable itemCount = memory.Count
    
    interface IStorageBackend<'K, 'V> with
        member _.StoreBatch(updates: ('K * 'V * int64) seq) =
            task {
                let mutable newMemory = memory
                let mutable toSpill = []
                
                for (key, value, weight) in updates do
                    match HashMap.tryFind key newMemory with
                    | Some (existingValue, existingWeight) ->
                        let newWeight = existingWeight + weight
                        if newWeight = 0L then
                            newMemory <- HashMap.remove key newMemory
                            itemCount <- itemCount - 1
                        else
                            newMemory <- HashMap.add key (existingValue, newWeight) newMemory
                    | None ->
                        if weight <> 0L then
                            newMemory <- HashMap.add key (value, weight) newMemory
                            itemCount <- itemCount + 1
                    
                    // Check if we need to spill
                    if itemCount > spillThreshold then
                        toSpill <- (key, value, weight) :: toSpill
                
                memory <- newMemory
                
                // Spill to disk if necessary
                if not (List.isEmpty toSpill) then
                    do! diskBackend.StoreBatch(toSpill)
                    for (k, _, _) in toSpill do
                        memory <- HashMap.remove k memory
                        itemCount <- itemCount - 1
            }
        
        member _.Get(key: 'K) =
            task {
                match HashMap.tryFind key memory with
                | Some value -> return Some value
                | None -> return! diskBackend.Get(key)
            }
        
        member _.GetIterator() =
            task {
                let! diskIter = diskBackend.GetIterator()
                let memoryIter = 
                    memory
                    |> HashMap.toSeq
                    |> Seq.map (fun (k, (v, w)) -> (k, v, w))
                
                // Merge memory and disk iterators
                return Seq.append memoryIter diskIter
            }
        
        member _.GetRangeIterator(startKey: 'K option, endKey: 'K option) =
            task {
                let! diskIter = diskBackend.GetRangeIterator(startKey, endKey)
                
                let memoryIter = 
                    memory
                    |> HashMap.toSeq
                    |> Seq.filter (fun (k, _) ->
                        let afterStart = 
                            match startKey with
                            | Some start -> k >= start
                            | None -> true
                        let beforeEnd =
                            match endKey with
                            | Some end' -> k <= end'
                            | None -> true
                        afterStart && beforeEnd
                    )
                    |> Seq.map (fun (k, (v, w)) -> (k, v, w))
                
                return Seq.append memoryIter diskIter
            }
        
        member _.Compact() =
            task {
                // First spill all memory to disk
                let updates = 
                    memory
                    |> HashMap.toSeq
                    |> Seq.map (fun (k, (v, w)) -> (k, v, w))
                
                do! diskBackend.StoreBatch(updates)
                memory <- HashMap.empty
                itemCount <- 0
                
                // Then compact disk
                do! diskBackend.Compact()
            }
        
        member _.GetStats() =
            diskBackend.GetStats()
        
        member _.Dispose() =
            diskBackend.Dispose()