module DBSP.Tests.Unit.CheckpointingTests

open System
open NUnit.Framework
open System.Threading.Tasks
open DBSP.Circuit
open DBSP.Operators.Interfaces

type CounterOp(?name: string) =
    inherit BaseOperator(defaultArg name "Counter")
    let mutable count = 0
    member _.Inc() = count <- count + 1
    member _.Value = count
    interface IStatefulOperator<int> with
        member _.GetState() = count
        member _.SetState(s) = count <- s
        member _.SerializeState() = task { return BitConverter.GetBytes(count) }
        member _.DeserializeState(bytes: byte[]) = task { count <- BitConverter.ToInt32(bytes, 0) }

[<Test>]
let ``Checkpoint and restore stateful operator`` () =
    let (circuit, _) = RootCircuit.Build(fun b ->
        let op = CounterOp()
        // Register as generic operator in circuit for checkpoint manager to discover
        let id = b.AddOperator(op, { Id = 1L; Name = "Counter"; OperatorType = "Counter"; InputTypes = []; OutputType = typeof<int>; IsAsync = false; IsStateful = true })
        (op, id))
    let cfg = { RuntimeConfig.Default with EnableCheckpointing = true; StoragePath = Some (System.IO.Path.Combine(System.IO.Path.GetTempPath(), "dbsp_ckpt")) }
    let rt = CircuitRuntimeModule.create circuit cfg
    match rt.Start() with | Ok () -> () | _ -> Assert.Fail()
    // Simulate updates
    let op =
        match circuit.Operators |> Map.toList |> List.head with
        | _, o -> o :?> CounterOp
    op.Inc(); op.Inc()
    // Create checkpoint
    let res = rt.CreateCheckpointAsync("test") |> Async.AwaitTask |> Async.RunSynchronously
    match res with | Ok () -> () | Error e -> Assert.Fail(e)
    // Mutate further
    op.Inc()
    Assert.That(op.Value, Is.EqualTo 3)
    // Restore
    let res2 = rt.RestoreCheckpointAsync(rt.CurrentEpoch, "test") |> Async.AwaitTask |> Async.RunSynchronously
    match res2 with | Ok () -> () | Error e -> Assert.Fail(e)
    // Expect restored value to be 2
    Assert.That(op.Value, Is.EqualTo 2)

