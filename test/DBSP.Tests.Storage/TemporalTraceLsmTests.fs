module DBSP.Tests.Storage.TemporalTraceLsmTests

open NUnit.Framework
open DBSP.Storage

let mkConfig() =
    { DataPath = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "dbsp_temporal_" + string (System.Guid.NewGuid()))
      MaxMemoryBytes = 10_000_000L
      CompactionThreshold = 32
      WriteBufferSize = 1024
      BlockCacheSize = 1_000_000L
      SpillThreshold = 0.8 }

[<Test>]
let ``TemporalTrace persists batches by time and snapshots correctly`` () = task {
    let cfg = mkConfig()
    System.IO.Directory.CreateDirectory(cfg.DataPath) |> ignore
    let ser = MessagePackSerializer<System.ValueTuple<int64,int,string>>() :> ISerializer<System.ValueTuple<int64,int,string>>
    use trace = new LSMTemporalTrace<int,string>(cfg, ser)
    do! trace.InsertBatch(1L, [ (1, "a", 1L); (2, "b", 1L) ])
    do! trace.InsertBatch(2L, [ (1, "a", -1L); (3, "c", 2L) ])
    let! snap1 = trace.QueryAtTime(1L)
    let arr1 = snap1 |> Seq.toArray
    Assert.That(arr1 |> Array.exists (fun struct(k,v,_) -> k=1 && v="a"), Is.True)
    Assert.That(arr1 |> Array.exists (fun struct(k,v,_) -> k=2 && v="b"), Is.True)
    let! snap2 = trace.QueryAtTime(2L)
    let arr2 = snap2 |> Seq.toArray
    Assert.That(arr2 |> Array.exists (fun struct(k,v,_) -> k=1 && v="a"), Is.False)
    Assert.That(arr2 |> Array.exists (fun struct(k,v,_) -> k=3 && v="c"), Is.True)
}

[<Test>]
let ``TemporalTrace returns per-time ranges`` () = task {
    let cfg = mkConfig()
    System.IO.Directory.CreateDirectory(cfg.DataPath) |> ignore
    let ser = MessagePackSerializer<System.ValueTuple<int64,int,int>>() :> ISerializer<System.ValueTuple<int64,int,int>>
    use trace = new LSMTemporalTrace<int,int>(cfg, ser)
    do! trace.InsertBatch(10L, [ (1, 1, 1L); (1, 2, 1L) ])
    do! trace.InsertBatch(12L, [ (2, 1, 1L) ])
    let! range = trace.QueryTimeRange(9L, 11L)
    let arr = range |> Seq.toArray
    Assert.That(arr.Length, Is.EqualTo 1)
    let struct(t, entries) = arr.[0]
    Assert.That(t, Is.EqualTo 10L)
    Assert.That(entries.Length, Is.EqualTo 2)
}

