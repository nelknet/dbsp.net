module DBSP.Tests.Storage.TemporalStorageTests

open NUnit.Framework
open DBSP.Storage

let mkConfig tmp =
    { DataPath = tmp
      MaxMemoryBytes = 10_000_000L
      CompactionThreshold = 4
      WriteBufferSize = 1024
      BlockCacheSize = 1_000_000L
      SpillThreshold = 0.8 }

[<Test>]
let ``Temporal spine accumulates across time`` () = task {
    let tmp = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "dbsp_temp_" + string (System.Guid.NewGuid()))
    System.IO.Directory.CreateDirectory(tmp) |> ignore
    let config = mkConfig tmp
    let ser = MessagePackSerializer<struct (int * string)>() :> ISerializer<struct (int * string)>
    let spine = TemporalSpine<int,string>(config, ser)
    do! spine.InsertBatch(1L, [| struct (1, "A", 1L); struct (2, "B", 1L) |])
    do! spine.InsertBatch(2L, [| struct (1, "A", 1L); struct (3, "C", -1L) |])
    let! at2 = spine.QueryAtTime 2L
    Assert.That(at2 |> Array.exists (fun struct (k,_,_) -> k = 1), Is.True)
}

[<Test>]
let ``Stores multiple time versions and queries at times`` () = task {
    let tmp = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "dbsp_temp_" + string (System.Guid.NewGuid()))
    System.IO.Directory.CreateDirectory(tmp) |> ignore
    let config = mkConfig tmp
    let ser = MessagePackSerializer<struct (int * string)>() :> ISerializer<struct (int * string)>
    let spine = TemporalSpine<int,string>(config, ser)
    do! spine.InsertBatch(1L, [| struct (1, "V1", 1L); struct (2, "V2", 1L) |])
    do! spine.InsertBatch(2L, [| struct (1, "V1_Updated", 1L); struct (3, "V3", 1L) |])
    do! spine.InsertBatch(3L, [| struct (2, "V2", -1L); struct (4, "V4", 1L) |])
    let! t1 = spine.QueryAtTime 1L
    let! t2 = spine.QueryAtTime 2L
    let! t3 = spine.QueryAtTime 3L
    Assert.That(t1.Length, Is.EqualTo 2)
    Assert.That(t1 |> Array.exists (fun struct (k,v,w) -> k=1 && v="V1" && w=1L), Is.True)
    Assert.That(t2 |> Array.exists (fun struct (k,v,w) -> k=1 && v="V1_Updated" && w=1L), Is.True)
    Assert.That(t3 |> Array.exists (fun struct (k,v,w) -> k=4 && v="V4" && w=1L), Is.True)
    Assert.That(t3 |> Array.exists (fun struct (k,_,_) -> k=2), Is.False)
}

[<Test>]
let ``Compacts old batches and preserves later state`` () = task {
    let tmp = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "dbsp_temp_" + string (System.Guid.NewGuid()))
    System.IO.Directory.CreateDirectory(tmp) |> ignore
    let config = mkConfig tmp
    let ser = MessagePackSerializer<struct (int * string)>() :> ISerializer<struct (int * string)>
    let spine = TemporalSpine<int,string>(config, ser)
    for t in 1L .. 10L do
        do! spine.InsertBatch(t, [| struct (int t, $"Value_{t}", 1L) |])
    do! spine.Compact 5L
    let! before = spine.QueryAtTime 3L
    let! after = spine.QueryAtTime 7L
    Assert.That(before.Length, Is.EqualTo 3)
    Assert.That(after.Length, Is.EqualTo 7)
}

[<Test>]
let ``Compaction preserves query results`` () = task {
    let tmp = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "dbsp_temp_" + string (System.Guid.NewGuid()))
    System.IO.Directory.CreateDirectory(tmp) |> ignore
    let config = mkConfig tmp
    let ser = MessagePackSerializer<struct (int * string)>() :> ISerializer<struct (int * string)>
    let spine = TemporalSpine<int,string>(config, ser)
    // Insert duplicates across times and within a time bucket
    do! spine.InsertBatch(1L, [| struct (1, "A", 1L); struct (1, "A", -1L); struct (2, "B", 2L) |])
    do! spine.InsertBatch(2L, [| struct (2, "B", -1L); struct (3, "C", 1L) |])
    let! before = spine.QueryAtTime 2L
    do! spine.Compact 10L
    let! after = spine.QueryAtTime 2L
    // Results should be the same pre/post compaction
    let normalize (arr: struct (int*string*int64) array) =
        arr |> Array.sortBy (fun struct (k,v,w) -> k,v,w)
    Assert.That(normalize after, Is.EqualTo (normalize before))
}

[<Test>]
let ``Handles time range queries`` () = task {
    let tmp = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "dbsp_temp_" + string (System.Guid.NewGuid()))
    System.IO.Directory.CreateDirectory(tmp) |> ignore
    let config = mkConfig tmp
    let ser = MessagePackSerializer<struct (int * string)>() :> ISerializer<struct (int * string)>
    let spine = TemporalSpine<int,string>(config, ser)
    do! spine.InsertBatch(1L, [| struct (1, "T1", 1L) |])
    do! spine.InsertBatch(2L, [| struct (2, "T2", 1L) |])
    do! spine.InsertBatch(3L, [| struct (3, "T3", 1L) |])
    do! spine.InsertBatch(4L, [| struct (4, "T4", 1L) |])
    do! spine.InsertBatch(5L, [| struct (5, "T5", 1L) |])
    let! range = spine.QueryTimeRange(2L, 4L)
    Assert.That(range.Length, Is.EqualTo 3)
}

[<Test>]
let ``Merges overlapping batches correctly`` () = task {
    let tmp = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "dbsp_temp_" + string (System.Guid.NewGuid()))
    System.IO.Directory.CreateDirectory(tmp) |> ignore
    let config = mkConfig tmp
    let ser = MessagePackSerializer<struct (int * string)>() :> ISerializer<struct (int * string)>
    let spine = TemporalSpine<int,string>(config, ser)
    let b1 = [| struct (1, "A", 1L); struct (2, "B", 2L); struct (3, "C", 1L) |]
    let b2 = [| struct (2, "B", 1L); struct (3, "C", -1L); struct (4, "D", 1L) |]
    let b3 = [| struct (1, "A", -1L); struct (4, "D", 1L); struct (5, "E", 1L) |]
    do! spine.InsertBatch(1L, b1)
    do! spine.InsertBatch(2L, b2)
    do! spine.InsertBatch(3L, b3)
    let! merged = spine.QueryAtTime 3L
    Assert.That(merged |> Array.exists (fun struct (k,_,_) -> k=1), Is.False)
    Assert.That(merged |> Array.exists (fun struct (k,v,w) -> k=2 && v="B" && w=3L), Is.True)
    Assert.That(merged |> Array.exists (fun struct (k,_,_) -> k=3), Is.False)
    Assert.That(merged |> Array.exists (fun struct (k,v,w) -> k=4 && v="D" && w=2L), Is.True)
    Assert.That(merged |> Array.exists (fun struct (k,v,w) -> k=5 && v="E" && w=1L), Is.True)
}

[<Test>]
let ``Handles large time jumps and empties`` () = task {
    let tmp = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "dbsp_temp_" + string (System.Guid.NewGuid()))
    System.IO.Directory.CreateDirectory(tmp) |> ignore
    let config = mkConfig tmp
    let ser = MessagePackSerializer<struct (int * string)>() :> ISerializer<struct (int * string)>
    let spine = TemporalSpine<int,string>(config, ser)
    do! spine.InsertBatch(100L, [| struct (1, "Early", 1L) |])
    do! spine.InsertBatch(1_000_000L, [| struct (2, "Late", 1L) |])
    let! early = spine.QueryAtTime 100L
    let! mid = spine.QueryAtTime 500_000L
    let! late = spine.QueryAtTime 1_000_000L
    Assert.That(early.Length, Is.EqualTo 1)
    Assert.That(mid.Length, Is.EqualTo 1)
    Assert.That(late.Length, Is.EqualTo 2)
    // Empty ranges
    let! before = spine.QueryAtTime 5L
    let! range = spine.QueryTimeRange(1L, 9L)
    Assert.That(before.Length, Is.EqualTo 0)
    Assert.That(range.Length, Is.EqualTo 0)
}

[<Test>]
let ``Preserves frontier (no strict enforcement)`` () = task {
    let tmp = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "dbsp_temp_" + string (System.Guid.NewGuid()))
    System.IO.Directory.CreateDirectory(tmp) |> ignore
    let config = mkConfig tmp
    let ser = MessagePackSerializer<struct (int * string)>() :> ISerializer<struct (int * string)>
    let spine = TemporalSpine<int,string>(config, ser)
    do! spine.InsertBatch(5L, [| struct (1, "V1", 1L) |])
    do! spine.InsertBatch(10L, [| struct (2, "V2", 1L) |])
    do! spine.InsertBatch(15L, [| struct (3, "V3", 1L) |])
    do! spine.AdvanceFrontier 10L
    do! spine.InsertBatch(7L, [| struct (4, "V4", 1L) |])
    let! r = spine.QueryAtTime 7L
    Assert.That(r, Is.Not.Null)
}
