module DBSP.Tests.Storage.LSMRangeEdgeTests

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
let ``Empty range when start greater than end`` () = task {
    let tmp = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "dbsp_lsm_range_" + string (System.Guid.NewGuid()))
    System.IO.Directory.CreateDirectory(tmp) |> ignore
    let cfg = mkConfig tmp
    let ser = MessagePackSerializer<System.ValueTuple<int,int>>() :> ISerializer<System.ValueTuple<int,int>>
    let lsm = LSMStorageBackend<int,int>(cfg, ser)
    do! lsm.StoreBatchWithFlush([ for i in 1 .. 10 -> (i, i, 1L) ], true)
    let! it = (lsm :> IStorageBackend<int,int>).GetRangeIterator(Some 8) (Some 3)
    Assert.That(it |> Seq.length, Is.EqualTo 0)
}

[<Test>]
let ``Start None returns items up to end inclusive`` () = task {
    let tmp = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "dbsp_lsm_range_" + string (System.Guid.NewGuid()))
    System.IO.Directory.CreateDirectory(tmp) |> ignore
    let cfg = mkConfig tmp
    let ser = MessagePackSerializer<System.ValueTuple<int,int>>() :> ISerializer<System.ValueTuple<int,int>>
    let lsm = LSMStorageBackend<int,int>(cfg, ser)
    do! lsm.StoreBatchWithFlush([ for i in 1 .. 10 -> (i, i, 1L) ], true)
    let! it = (lsm :> IStorageBackend<int,int>).GetRangeIterator(None) (Some 5)
    let arr = it |> Seq.map (fun (k,_,_) -> k) |> Seq.toArray
    Assert.That(arr, Is.EqualTo [|1;2;3;4;5|])
}

[<Test>]
let ``End None returns items from start inclusive`` () = task {
    let tmp = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "dbsp_lsm_range_" + string (System.Guid.NewGuid()))
    System.IO.Directory.CreateDirectory(tmp) |> ignore
    let cfg = mkConfig tmp
    let ser = MessagePackSerializer<System.ValueTuple<int,int>>() :> ISerializer<System.ValueTuple<int,int>>
    let lsm = LSMStorageBackend<int,int>(cfg, ser)
    do! lsm.StoreBatchWithFlush([ for i in 1 .. 7 -> (i, i, 1L) ], true)
    let! it = (lsm :> IStorageBackend<int,int>).GetRangeIterator(Some 4) None
    let arr = it |> Seq.map (fun (k,_,_) -> k) |> Seq.toArray
    Assert.That(arr, Is.EqualTo [|4;5;6;7|])
}

[<Test>]
let ``Range over non-existent start still returns greater keys`` () = task {
    let tmp = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "dbsp_lsm_range_" + string (System.Guid.NewGuid()))
    System.IO.Directory.CreateDirectory(tmp) |> ignore
    let cfg = mkConfig tmp
    let ser = MessagePackSerializer<System.ValueTuple<int,int>>() :> ISerializer<System.ValueTuple<int,int>>
    let lsm = LSMStorageBackend<int,int>(cfg, ser)
    do! lsm.StoreBatchWithFlush([ 2, 20, 1L; 4, 40, 1L; 6, 60, 1L ], true)
    let! it = (lsm :> IStorageBackend<int,int>).GetRangeIterator(Some 3) (Some 5)
    let arr = it |> Seq.map (fun (k,_,_) -> k) |> Seq.toArray
    Assert.That(arr, Is.EqualTo [|4|])
}
