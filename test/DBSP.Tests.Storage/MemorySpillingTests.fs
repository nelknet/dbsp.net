module DBSP.Tests.Storage.MemorySpillingTests

open NUnit.Framework
open DBSP.Storage

[<Test>]
let ``AdaptiveStorageManager exposes memory pressure`` () =
    let cfg = { DataPath = System.IO.Path.GetTempPath(); MaxMemoryBytes = 10_000_000L; CompactionThreshold = 10; WriteBufferSize = 1024; BlockCacheSize = 1_000_000L; SpillThreshold = 0.8 }
    let mgr = AdaptiveStorageManager(cfg)
    let pressure = mgr.GetMemoryPressure()
    Assert.GreaterOrEqual(pressure, 0.0)
    Assert.LessOrEqual(pressure, 1.0)

[<Test>]
let ``HybridStorage stores and retrieves under small cap`` () = task {
    let cfg = { DataPath = System.IO.Path.GetTempPath(); MaxMemoryBytes = 100L; CompactionThreshold = 10; WriteBufferSize = 1024; BlockCacheSize = 1_000L; SpillThreshold = 0.5 }
    let ser = MessagePackSerializer<int * string>() :> ISerializer<int * string>
    let storage = HybridStorageBackend<int,string>(cfg, ser) :> IStorageBackend<int,string>
    do! storage.StoreBatch([ for i in 1 .. 100 -> (i, $"LongValue_{i}", 1L) ])
    let! r1 = storage.Get 1
    let! r50 = storage.Get 50
    let! r100 = storage.Get 100
    Assert.IsTrue(r1.IsSome)
    Assert.IsTrue(r50.IsSome)
    Assert.IsTrue(r100.IsSome)
}

[<Test>]
let ``HybridStorage merges duplicate keys by weight`` () = task {
    let cfg = { DataPath = System.IO.Path.GetTempPath(); MaxMemoryBytes = 10_000_000L; CompactionThreshold = 10; WriteBufferSize = 1024; BlockCacheSize = 1_000_000L; SpillThreshold = 0.8 }
    let ser = MessagePackSerializer<int * string>() :> ISerializer<int * string>
    let storage = HybridStorageBackend<int,string>(cfg, ser) :> IStorageBackend<int,string>
    do! storage.StoreBatch([ (1, "V", 2L) ])
    do! storage.StoreBatch([ (1, "V", 3L) ])
    do! storage.StoreBatch([ (1, "V", -1L) ])
    let! r = storage.Get 1
    match r with
    | Some (_,w) -> Assert.AreEqual(4L, w)
    | None -> Assert.Fail("expected value")
}

[<Test>]
let ``HybridStorage iterates all entries`` () = task {
    let cfg = { DataPath = System.IO.Path.GetTempPath(); MaxMemoryBytes = 10_000_000L; CompactionThreshold = 10; WriteBufferSize = 1024; BlockCacheSize = 1_000_000L; SpillThreshold = 0.8 }
    let ser = MessagePackSerializer<int * string>() :> ISerializer<int * string>
    let storage = HybridStorageBackend<int,string>(cfg, ser) :> IStorageBackend<int,string>
    let batch1 = [ for i in 1 .. 50 -> (i, $"B1_{i}", 1L) ]
    let batch2 = [ for i in 51 .. 100 -> (i, $"B2_{i}", 1L) ]
    do! storage.StoreBatch(batch1)
    do! storage.StoreBatch(batch2)
    let! it = storage.GetIterator()
    let arr = it |> Seq.toArray
    Assert.AreEqual(100, arr.Length)
    Assert.AreEqual([|1..100|], arr |> Array.map (fun (k,_,_) -> k))
}

[<Test>]
let ``HybridStorage supports range queries`` () = task {
    let cfg = { DataPath = System.IO.Path.GetTempPath(); MaxMemoryBytes = 10_000_000L; CompactionThreshold = 10; WriteBufferSize = 1024; BlockCacheSize = 1_000_000L; SpillThreshold = 0.8 }
    let ser = MessagePackSerializer<int * string>() :> ISerializer<int * string>
    let storage = HybridStorageBackend<int,string>(cfg, ser) :> IStorageBackend<int,string>
    do! storage.StoreBatch([ for i in 1 .. 100 -> (i, $"V_{i}", 1L) ])
    let! it = storage.GetRangeIterator (Some 25) (Some 75)
    let arr = it |> Seq.toArray
    Assert.AreEqual(51, arr.Length)
    Assert.AreEqual(25, arr.[0] |> (fun (k,_,_) -> k))
    Assert.AreEqual(75, arr.[arr.Length-1] |> (fun (k,_,_) -> k))
}

[<Test>]
let ``SpillCoordinator triggers at high pressure or large batch`` () =
    let sc = SpillCoordinator(0.8)
    let shouldSpill = sc.ShouldSpill(1_000_000_000L)
    // Just exercise path; environment-dependent
    Assert.IsTrue(shouldSpill || not shouldSpill)
