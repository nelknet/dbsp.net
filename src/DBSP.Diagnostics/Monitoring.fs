namespace DBSP.Diagnostics

open System
open System.Threading.Tasks
open DBSP.Storage

type StorageSnapshot = {
    Timestamp: DateTimeOffset
    BytesWritten: int64
    BytesRead: int64
    KeysStored: int64
    Compactions: int
}

module Monitoring =
    let collectStorageStats (backend: IStorageBackend<'K,'V>) = task {
        let! s = backend.GetStats()
        return {
            Timestamp = DateTimeOffset.UtcNow
            BytesWritten = s.BytesWritten
            BytesRead = s.BytesRead
            KeysStored = s.KeysStored
            Compactions = s.CompactionCount
        }
    }

