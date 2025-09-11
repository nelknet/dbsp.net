module DBSP.Tests.Storage.TemporalSnapshotCircuitTests

open NUnit.Framework
open DBSP.Storage
open DBSP.Circuit

[<Test>]
let ``Circuit clock + snapshot operator emits snapshots`` () = task {
    // Arrange: temporal trace with simple data
    let cfg = { DataPath = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "dbsp_circuit_temporal_" + string (System.Guid.NewGuid()))
                MaxMemoryBytes = 10_000_000L; CompactionThreshold = 32; WriteBufferSize = 1024; BlockCacheSize = 1_000_000L; SpillThreshold = 0.8 }
    System.IO.Directory.CreateDirectory(cfg.DataPath) |> ignore
    use trace = new LSMTemporalTrace<int,string>(cfg, MessagePackSerializer<System.ValueTuple<int64,int,string>>() :> ISerializer<System.ValueTuple<int64,int,string>>)
    do! trace.InsertBatch(1L, [ (1, "a", 1L); (2, "b", 1L) ])
    do! trace.InsertBatch(3L, [ (2, "b", -1L); (3, "c", 2L) ])

    // Build circuit with clock and snapshot operator
    let (circuit, snapshotHandle) =
        RootCircuit.Build(fun b ->
            let clock = b.AddClock("clock")
            let out = b.AddSnapshot("snapshot", trace :> ITemporalTrace<int,string>, clock)
            out)

    let runtime = CircuitRuntimeModule.create circuit { RuntimeConfig.Default with MaintenanceEverySteps = 0L }

    // Act + Assert across steps
    let stepOk1 = runtime.Start()
    Assert.That(stepOk1.IsOk, Is.True)
    let r1 = runtime.ExecuteStepAsync() |> Async.AwaitTask |> Async.RunSynchronously
    Assert.That(r1.IsOk, Is.True)
    match snapshotHandle.Value with
    | Some arr1 ->
        // time=1 snapshot should include (1,a) and (2,b)
        Assert.That(arr1 |> Array.exists (fun struct(k,v,_) -> k=1 && v="a"), Is.True)
        Assert.That(arr1 |> Array.exists (fun struct(k,v,_) -> k=2 && v="b"), Is.True)
    | None -> Assert.Fail("Expected snapshot output at t=1")

    let r2 = runtime.ExecuteStepAsync() |> Async.AwaitTask |> Async.RunSynchronously
    Assert.That(r2.IsOk, Is.True)
    // At t=2, same as t=1
    match snapshotHandle.Value with
    | Some arr2 ->
        Assert.That(arr2 |> Array.exists (fun struct(k,v,_) -> k=1 && v="a"), Is.True)
        Assert.That(arr2 |> Array.exists (fun struct(k,v,_) -> k=2 && v="b"), Is.True)
    | None -> Assert.Fail("Expected snapshot output at t=2")

    let r3 = runtime.ExecuteStepAsync() |> Async.AwaitTask |> Async.RunSynchronously
    Assert.That(r3.IsOk, Is.True)
    // At t=3, (2,b) deleted; (3,c) present
    match snapshotHandle.Value with
    | Some arr3 ->
        Assert.That(arr3 |> Array.exists (fun struct(k,v,_) -> k=2 && v="b"), Is.False)
        Assert.That(arr3 |> Array.exists (fun struct(k,v,_) -> k=3 && v="c"), Is.True)
    | None -> Assert.Fail("Expected snapshot output at t=3")
}

