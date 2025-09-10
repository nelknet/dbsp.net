module DBSP.Tests.Storage.LSMStorageTests

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
let ``ZoneTree-backed storage stores and retrieves`` () = task {
    let tmp = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "dbsp_lsm_" + string (System.Guid.NewGuid()))
    System.IO.Directory.CreateDirectory(tmp) |> ignore
    let config = mkConfig tmp
    let ser = MessagePackSerializer<struct (int * string)>() :> ISerializer<struct (int * string)>
    let storage = LSMStorageBackend<int,string>(config, ser) :> IStorageBackend<int,string>
    do! storage.StoreBatch([ (1, "One", 1L); (2, "Two", 1L); (3, "Three", 1L) ])
    do! storage.Compact()
    let! r1 = storage.Get 1
    let! r4 = storage.Get 4
    match r1 with
    | Some (v,w) -> Assert.That(v, Is.EqualTo "One"); Assert.That(w, Is.EqualTo 1L)
    | None -> Assert.Fail("expected value")
    Assert.That(r4.IsNone, Is.True)
}

[<Test>]
let ``Weight cancellation removes key`` () = task {
    let tmp = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "dbsp_lsm_" + string (System.Guid.NewGuid()))
    System.IO.Directory.CreateDirectory(tmp) |> ignore
    let config = mkConfig tmp
    let ser = MessagePackSerializer<struct (int * string)>() :> ISerializer<struct (int * string)>
    let storage = LSMStorageBackend<int,string>(config, ser) :> IStorageBackend<int,string>
    do! storage.StoreBatch([ (1, "X", 2L) ])
    do! storage.StoreBatch([ (1, "X", -2L) ])
    do! storage.Compact()
    let! r = storage.Get 1
    Assert.That(r.IsNone, Is.True)
}

[<Test>]
let ``Iterator returns all expected entries and filters zero-weight`` () = task {
    let tmp = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "dbsp_lsm_" + string (System.Guid.NewGuid()))
    System.IO.Directory.CreateDirectory(tmp) |> ignore
    let config = mkConfig tmp
    let ser = MessagePackSerializer<struct (int * string)>() :> ISerializer<struct (int * string)>
    let storage = LSMStorageBackend<int,string>(config, ser) :> IStorageBackend<int,string>
    do! storage.StoreBatch([ (1, "One", 1L)
                             (2, "Two", 2L)
                             (3, "Three", 3L)
                             (4, "Four", 0L)
                             (5, "Five", -1L) ])
    do! storage.Compact()
    let! it = storage.GetIterator()
    let arr = it |> Seq.toArray
    // Expect 4 entries (4 with 0 weight should not appear)
    Assert.That(arr.Length, Is.EqualTo 4)
    Assert.That(arr |> Array.exists (fun (k,v,w) -> k=1 && v="One" && w=1L), Is.True)
    Assert.That(arr |> Array.exists (fun (k,v,w) -> k=2 && v="Two" && w=2L), Is.True)
    Assert.That(arr |> Array.exists (fun (k,v,w) -> k=3 && v="Three" && w=3L), Is.True)
    Assert.That(arr |> Array.exists (fun (k,v,w) -> k=5 && v="Five" && w=(-1L)), Is.True)
}

[<Test>]
let ``Range queries return bounded keys`` () = task {
    let tmp = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "dbsp_lsm_" + string (System.Guid.NewGuid()))
    System.IO.Directory.CreateDirectory(tmp) |> ignore
    let config = mkConfig tmp
    let ser = MessagePackSerializer<struct (int * string)>() :> ISerializer<struct (int * string)>
    let storage = LSMStorageBackend<int,string>(config, ser) :> IStorageBackend<int,string>
    do! storage.StoreBatch([ for i in 1 .. 10 -> (i, $"Value{i}", 1L) ])
    do! storage.Compact()
    let! it = storage.GetRangeIterator(Some 3) (Some 7)
    let arr = it |> Seq.toArray
    Assert.That(arr.Length, Is.EqualTo 5)
    let keys = arr |> Array.map (fun (k,_,_) -> k)
    Assert.That(keys, Is.EqualTo [|3;4;5;6;7|])
}

[<Test>]
let ``Compaction updates statistics`` () = task {
    let tmp = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "dbsp_lsm_" + string (System.Guid.NewGuid()))
    System.IO.Directory.CreateDirectory(tmp) |> ignore
    let config = mkConfig tmp
    let ser = MessagePackSerializer<struct (int * string)>() :> ISerializer<struct (int * string)>
    let storage = LSMStorageBackend<int,string>(config, ser) :> IStorageBackend<int,string>
    for i in 0 .. 9 do
        do! storage.StoreBatch([ for j in (i*10) .. (i*10 + 9) -> (j, $"Value{j}", 1L) ])
    do! storage.Compact()
    let! stats = storage.GetStats()
    Assert.That(stats.CompactionCount, Is.GreaterThan 0)
    Assert.That(stats.LastCompactionTime.IsSome, Is.True)
}

[<Test>]
let ``Stats track bytes and keys`` () = task {
    let tmp = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "dbsp_lsm_" + string (System.Guid.NewGuid()))
    System.IO.Directory.CreateDirectory(tmp) |> ignore
    let config = mkConfig tmp
    let ser = MessagePackSerializer<struct (int * string)>() :> ISerializer<struct (int * string)>
    let storage = LSMStorageBackend<int,string>(config, ser) :> IStorageBackend<int,string>
    do! storage.StoreBatch([ (1, "One", 1L); (2, "Two", 1L) ])
    do! storage.Compact()
    let! _ = storage.Get 1
    let! _ = storage.Get 2
    let! stats = storage.GetStats()
    Assert.That(stats.BytesWritten, Is.GreaterThan 0L)
    Assert.That(stats.BytesRead, Is.GreaterThan 0L)
    Assert.That(stats.KeysStored, Is.EqualTo 2L)
}
