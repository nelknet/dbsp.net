module DBSP.Tests.Storage.LSMBoundaryTests

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
let ``Range over K includes negative V values`` () = task {
    let tmp = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "dbsp_lsm_bounds_" + string (System.Guid.NewGuid()))
    System.IO.Directory.CreateDirectory(tmp) |> ignore
    let config = mkConfig tmp
    let ser = MessagePackSerializer<System.ValueTuple<int,int>>() :> ISerializer<System.ValueTuple<int,int>>
    let storage = LSMStorageBackend<int,int>(config, ser)
    // K=1 with negative and positive V
    do! storage.StoreBatchWithFlush([ (1, -5, 1L); (1, -1, 1L); (1, 0, 1L); (1, 3, 1L) ], true)
    // K=0 and K=2 noise
    do! storage.StoreBatchWithFlush([ (0, 100, 1L); (2, 200, 1L) ], true)
    let! it = (storage :> IStorageBackend<int,int>).GetRangeIterator(Some 1) (Some 1)
    let arr = it |> Seq.toArray
    Assert.AreEqual(4, arr.Length)
    let vs = arr |> Array.map (fun (_,v,_) -> v) |> Array.sort
    Assert.AreEqual([| -5; -1; 0; 3 |], vs)
}

[<Test>]
let ``Get returns some value for K even with negative V present`` () = task {
    let tmp = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "dbsp_lsm_bounds2_" + string (System.Guid.NewGuid()))
    System.IO.Directory.CreateDirectory(tmp) |> ignore
    let config = mkConfig tmp
    let ser = MessagePackSerializer<System.ValueTuple<int,int>>() :> ISerializer<System.ValueTuple<int,int>>
    let storage = LSMStorageBackend<int,int>(config, ser)
    do! storage.StoreBatchWithFlush([ (5, -10, 1L); (5, -2, 1L); (5, 7, 1L) ], true)
    let! r = (storage :> IStorageBackend<int,int>).Get 5
    Assert.IsTrue(r.IsSome)
}
