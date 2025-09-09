namespace DBSP.Circuit

open System.Threading.Tasks
open DBSP.Storage

/// Optional interface for operators that manage external state and can spill
type IStorageOperator =
    abstract member GetStateSize: unit -> int64
    abstract member Spill: unit -> Task

module StorageIntegration =
    /// Force a storage spill across all operators that support it
    let forceSpill (circuit: CircuitDefinition) (threshold: float) = task {
        let sc = SpillCoordinator(threshold)
        for kvp in circuit.Operators do
            match kvp.Value with
            | :? IStorageOperator as sop ->
                let stateSize = sop.GetStateSize()
                if sc.ShouldSpill(stateSize) then
                    do! sop.Spill()
            | _ -> ()
        return ()
    }

    /// Returns current memory pressure as ratio [0.0, 1.0]
    let getMemoryPressure () : float =
        let info = System.GC.GetGCMemoryInfo()
        let used = System.GC.GetTotalMemory(false)
        if info.TotalAvailableMemoryBytes <= 0L then 0.0
        else (float used) / (float info.TotalAvailableMemoryBytes)
