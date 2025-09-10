module DBSP.Tests.Storage.LSMPersistenceTests

open NUnit.Framework
open DBSP.Storage

let mkConfig tmp =
    { DataPath = tmp
      MaxMemoryBytes = 10_000_000L
      CompactionThreshold = 16
      WriteBufferSize = 1024
      BlockCacheSize = 1_000_000L
      SpillThreshold = 0.8 }

[<Test>]
let ``Persisted state survives reopen and matches pre-close state`` () = task {
    let tmp = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "dbsp_lsm_persist_" + string (System.Guid.NewGuid()))
    System.IO.Directory.CreateDirectory(tmp) |> ignore
    let cfg = mkConfig tmp
    let ser = MessagePackSerializer<System.ValueTuple<int,string>>() :> ISerializer<System.ValueTuple<int,string>>

    // First open: write some data and capture iterator
    let lsm1 = LSMStorageBackend<int,string>(cfg, ser)
    do! lsm1.StoreBatchWithFlush([ (1, "A", 1L); (2, "B", 2L); (1, "A", -1L); (3, "C", 3L) ], true)
    let! before = (lsm1 :> IStorageBackend<int,string>).GetIterator()
    let beforeArr = before |> Seq.toArray |> Array.sort
    do! (lsm1 :> IStorageBackend<int,string>).Dispose()

    // Reopen on same path: iterator should match
    let lsm2 = LSMStorageBackend<int,string>(cfg, ser)
    let! after = (lsm2 :> IStorageBackend<int,string>).GetIterator()
    let afterArr = after |> Seq.toArray |> Array.sort
    Assert.That(afterArr, Is.EqualTo beforeArr)
    do! (lsm2 :> IStorageBackend<int,string>).Dispose()
}
