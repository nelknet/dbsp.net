module DBSP.Tests.Storage.HybridManagerTests

open NUnit.Framework
open DBSP.Storage

let mkConfig sub =
    let baseDir = System.IO.Path.Combine(System.IO.Path.GetTempPath(), sub + "_" + string (System.Guid.NewGuid()))
    System.IO.Directory.CreateDirectory(baseDir) |> ignore
    { DataPath = baseDir
      MaxMemoryBytes = 256_000L
      CompactionThreshold = 8
      WriteBufferSize = 1024
      BlockCacheSize = 1_000_000L
      SpillThreshold = 0.05 }

[<Test>]
let ``AdaptiveStorageManager returns Hybrid backend when configured`` () = task {
    let cfg = mkConfig "hybrid_mgr"
    let mgr = AdaptiveStorageManager(cfg, StorageMode.Hybrid) :> IStorageManager
    let backend = mgr.GetBackend<int,string>()
    do! backend.StoreBatch([ (1, "a", 1L); (2, "b", 1L) ])
    let! r1 = backend.Get 1
    Assert.That(r1.IsSome, Is.True)
}

[<Test>]
let ``Hybrid stores and retrieves before spill`` () = task {
    let cfg = mkConfig "hybrid_small"
    let mgr = AdaptiveStorageManager(cfg, StorageMode.Hybrid) :> IStorageManager
    let storage = mgr.GetBackend<int,string>()
    // Small batch under thresholds
    do! storage.StoreBatch([ for i in 1 .. 10 -> (i, $"v{i}", 1L) ])
    let! it = storage.GetIterator()
    let arr = it |> Seq.toArray
    Assert.That(arr.Length, Is.EqualTo 10)
    let! g5 = storage.Get 5
    Assert.That(g5.IsSome, Is.True)
}

[<Test>]
let ``Hybrid spills to disk and preserves visibility`` () = task {
    let cfg = mkConfig "hybrid_spill"
    let mgr = AdaptiveStorageManager(cfg, StorageMode.Hybrid) :> IStorageManager
    let storage = mgr.GetBackend<int,string>()
    // Batch big enough to trigger spill
    do! storage.StoreBatch([ for i in 1 .. 500 -> (i, $"val{i}", 1L) ])
    let! it = storage.GetIterator()
    let arr = it |> Seq.toArray
    Assert.That(arr.Length, Is.EqualTo 500)
    let! g250 = storage.Get 250
    Assert.That(g250.IsSome, Is.True)
}

[<Test>]
let ``Hybrid overlays memory updates over disk`` () = task {
    let cfg = mkConfig "hybrid_overlay"
    let mgr = AdaptiveStorageManager(cfg, StorageMode.Hybrid) :> IStorageManager
    let storage = mgr.GetBackend<int,string>()
    // Spill a large batch
    do! storage.StoreBatch([ for i in 1 .. 300 -> (i, "disk", 1L) ])
    // Apply a small in-memory update that should remain buffered
    do! storage.StoreBatch([ (100, "mem", 1L); (101, "mem", 1L) ])
    let! g100 = storage.Get 100
    match g100 with
    | Some (v, _) -> Assert.That(v, Is.EqualTo "mem")
    | None -> Assert.Fail("expected value")
}

[<Test>]
let ``Hybrid range iterator respects bounds`` () = task {
    let cfg = mkConfig "hybrid_range"
    let mgr = AdaptiveStorageManager(cfg, StorageMode.Hybrid) :> IStorageManager
    let storage = mgr.GetBackend<int,string>()
    do! storage.StoreBatch([ for i in 1 .. 200 -> (i, $"v{i}", 1L) ])
    let! it = storage.GetRangeIterator (Some 50) (Some 150)
    let arr = it |> Seq.toArray
    Assert.That(arr.Length, Is.EqualTo 101)
    Assert.That(arr.[0] |> (fun (k,_,_) -> k), Is.EqualTo 50)
    Assert.That(arr.[arr.Length-1] |> (fun (k,_,_) -> k), Is.EqualTo 150)
}

[<Test>]
let ``Hybrid compact flushes memory and compacts disk`` () = task {
    let cfg = mkConfig "hybrid_compact"
    let mgr = AdaptiveStorageManager(cfg, StorageMode.Hybrid) :> IStorageManager
    let storage = mgr.GetBackend<int,string>()
    do! storage.StoreBatch([ for i in 1 .. 100 -> (i, "a", 1L) ])
    // Duplicate updates
    do! storage.StoreBatch([ for i in 1 .. 50 -> (i, "a", -1L) ])
    do! storage.Compact()
    let! it = storage.GetIterator()
    let arr = it |> Seq.toArray
    Assert.That(arr.Length, Is.EqualTo 50)
}

