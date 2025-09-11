module DBSP.Tests.Unit.ExactlyOnceTests

open NUnit.Framework
open System
open System.Threading.Tasks
open DBSP.Circuit
open DBSP.Operators.Interfaces

type SyntheticSource() =
    inherit BaseOperator("SyntheticSource")
    let mutable offset = 0L
    member _.EmitNext() =
        offset <- offset + 1L
        offset
    member _.Offset = offset
    interface IStatefulOperator<int64> with
        member _.GetState() = offset
        member _.SetState(s) = offset <- s
        member _.SerializeState() = task { return BitConverter.GetBytes(offset) }
        member _.DeserializeState(bytes: byte[]) = task { offset <- BitConverter.ToInt64(bytes, 0) }
    interface ICheckpointableSource with
        member _.GetOffset() = offset
        member _.SeekOffset(o) = offset <- o

[<Test>]
let ``Exactly-once with checkpoint and restore latest`` () =
    // Build a circuit with a single synthetic source
    let (c1, s1) = RootCircuit.Build(fun b ->
        let src = SyntheticSource()
        b.AddOperator(src, { Name = "src"; TypeInfo = "SyntheticSource"; Location = None }) |> ignore
        src)
    let basePath = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "dbsp_e2e_exactly_once")
    if System.IO.Directory.Exists(basePath) then System.IO.Directory.Delete(basePath, true)
    let cfg = { RuntimeConfig.Default with EnableCheckpointing = true; StoragePath = Some basePath }
    let rt1 = CircuitRuntimeModule.create c1 cfg
    match rt1.Start() with | Ok () -> () | Error e -> Assert.Fail(e)
    // Process first 5 records and checkpoint
    let produced1 = ResizeArray<int64>()
    for _ in 1 .. 5 do produced1.Add(s1.EmitNext())
    let resCk = rt1.CreateCheckpointAsync("e2e") |> Async.AwaitTask |> Async.RunSynchronously
    match resCk with | Ok () -> () | Error e -> Assert.Fail(e)
    // Process two more (not checkpointed)
    let _ = s1.EmitNext()
    let _ = s1.EmitNext()
    // Simulate crash by creating a new runtime/circuit and restoring latest checkpoint
    let (c2, s2) = RootCircuit.Build(fun b ->
        let src = SyntheticSource()
        b.AddOperator(src, { Name = "src"; TypeInfo = "SyntheticSource"; Location = None }) |> ignore
        src)
    let rt2 = CircuitRuntimeModule.create c2 cfg
    match rt2.RestoreLatestCheckpointAsync() |> Async.AwaitTask |> Async.RunSynchronously with
    | Ok () -> ()
    | Error e -> Assert.Fail(e)
    // Resume: next emissions should start from 6 (i.e., no duplicates for the first 5)
    let produced2 = ResizeArray<int64>()
    for _ in 1 .. 5 do produced2.Add(s2.EmitNext())
    // Validate exactly-once: first segment [1..5]; second resumes at 6..
    Assert.That(produced1.ToArray(), Is.EqualTo [|1L;2L;3L;4L;5L|])
    Assert.That(produced2.ToArray(), Is.EqualTo [|6L;7L;8L;9L;10L|])

