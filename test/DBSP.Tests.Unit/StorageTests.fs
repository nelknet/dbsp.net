namespace DBSP.Tests.Unit

open NUnit.Framework
open System.Threading.Tasks
open DBSP.Storage
open DBSP.Circuit

[<TestFixture>]
type StorageTests() =

    let mkConfig tmp =
        { DataPath = tmp
          MaxMemoryBytes = 10_000_000L
          CompactionThreshold = 4
          WriteBufferSize = 1024
          BlockCacheSize = 1_000_000L
          SpillThreshold = 0.8 }

    [<Test>]
    member _.``LSMStorage aggregates weights and eliminates zeros``() = task {
        let tmp = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "dbsp_lsmunit_" + string (System.Guid.NewGuid()))
        System.IO.Directory.CreateDirectory(tmp) |> ignore
        let config = mkConfig tmp
        let serializer = MessagePackSerializer<struct (int * string)>() :> ISerializer<struct (int * string)>
        let storage = InMemoryStorageBackend<int, string>(config) :> IStorageBackend<int,string>

        do! storage.StoreBatch([ (1, "One", 1L); (1, "One", -1L); (2, "Two", 3L) ])
        let! r1 = storage.Get 1
        let! r2 = storage.Get 2
        Assert.That(r1.IsNone, Is.True)
        match r2 with
        | Some (v,w) -> Assert.That(v, Is.EqualTo "Two"); Assert.That(w, Is.EqualTo 3L)
        | None -> Assert.Fail("expected value")
    }

    [<Test>]
    member _.``TemporalSpine accumulates by time``() = task {
        let tmp = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "dbsp_spineunit_" + string (System.Guid.NewGuid()))
        System.IO.Directory.CreateDirectory(tmp) |> ignore
        let config = mkConfig tmp
        let serializer = MessagePackSerializer<struct (int * string)>() :> ISerializer<struct (int * string)>
        let spine = TemporalSpine<int,string>(config, serializer)
        do! spine.InsertBatch(1L, [| struct (1, "A", 1L); struct (2, "B", 1L) |])
        do! spine.InsertBatch(2L, [| struct (1, "A", 1L); struct (3, "C", -1L) |])
        let! at1 = spine.QueryAtTime 1L
        let! at2 = spine.QueryAtTime 2L
        Assert.That(at1.Length, Is.EqualTo 2)
        Assert.That(at2 |> Array.exists (fun struct (k,_,_) -> k = 1), Is.True)
    }

    [<Test>]
    member _.``Runtime storage integration forceSpill triggers Spill on operator``() = task {
        // Arrange a fake operator implementing IStorageOperator
        let spillTriggered = ref false
        let fakeOp =
            { new IStorageOperator with
                member _.GetStateSize() = 1_000_000_000L // large
                member _.Spill() = spillTriggered := true; Task.CompletedTask }
        let build (b: CircuitBuilder) =
            let _ = b.AddOperator(fakeOp, { Name = "fake"; TypeInfo = "fake"; Location = None })
            ()
        let (circuit, _) = RootCircuit.Build(build)
        // Act
        do! StorageIntegration.forceSpill circuit 0.0
        // Assert
        Assert.That(!spillTriggered, Is.True)
    }
