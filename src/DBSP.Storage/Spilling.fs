namespace DBSP.Storage

open System

type SpillCoordinator(spillThresholdRatio: float) =
    member _.ShouldSpill(estimatedBytes: int64) =
        let info = GC.GetGCMemoryInfo()
        let used = GC.GetTotalMemory(false)
        let total = info.TotalAvailableMemoryBytes
        let pressure = float used / float total
        pressure >= spillThresholdRatio || float estimatedBytes >= 0.05 * float total

