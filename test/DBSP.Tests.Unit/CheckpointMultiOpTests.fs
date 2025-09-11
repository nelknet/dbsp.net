module DBSP.Tests.Unit.CheckpointMultiOpTests

open NUnit.Framework
open System
open System.Threading.Tasks
open DBSP.Circuit
open DBSP.Operators.Interfaces

type CounterOpA() =
    inherit BaseOperator("A")
    let mutable c = 0
    member _.Inc() = c <- c + 1
    member _.Value = c
    interface IStatefulOperator<int> with
        member _.GetState() = c
        member _.SetState(s) = c <- s
        member _.SerializeState() = task { return BitConverter.GetBytes(c) }
        member _.DeserializeState(bytes: byte[]) = task { c <- BitConverter.ToInt32(bytes, 0) }

type CounterOpB() =
    inherit BaseOperator("B")
    let mutable c = 100
    member _.Inc() = c <- c + 10
    member _.Value = c
    interface IStatefulOperator<int> with
        member _.GetState() = c
        member _.SetState(s) = c <- s
        member _.SerializeState() = task { return BitConverter.GetBytes(c) }
        member _.DeserializeState(bytes: byte[]) = task { c <- BitConverter.ToInt32(bytes, 0) }

[<Test>]
let ``Checkpoint captures multiple stateful operators`` () =
    let (circuit, (a,b)) = RootCircuit.Build(fun bldr ->
        let a = CounterOpA()
        let b = CounterOpB()
        bldr.AddOperator(a, { Name = "A"; TypeInfo = "Counter"; Location = None }) |> ignore
        bldr.AddOperator(b, { Name = "B"; TypeInfo = "Counter"; Location = None }) |> ignore
        (a,b))
    let cfg = { RuntimeConfig.Default with EnableCheckpointing = true; StoragePath = Some (System.IO.Path.Combine(System.IO.Path.GetTempPath(), "dbsp_ckpt_multi")) }
    let rt = CircuitRuntimeModule.create circuit cfg
    match rt.Start() with | Ok () -> () | _ -> Assert.Fail()
    a.Inc(); a.Inc(); b.Inc()
    let res = rt.CreateCheckpointAsync("multi") |> Async.AwaitTask |> Async.RunSynchronously
    match res with | Ok () -> () | Error e -> Assert.Fail(e)
    // mutate further
    a.Inc(); b.Inc()
    Assert.That(a.Value, Is.EqualTo 3)
    Assert.That(b.Value, Is.EqualTo 120)
    // restore
    let res2 = rt.RestoreCheckpointAsync(rt.CurrentEpoch, "multi") |> Async.AwaitTask |> Async.RunSynchronously
    match res2 with | Ok () -> () | Error e -> Assert.Fail(e)
    Assert.That(a.Value, Is.EqualTo 2)
    Assert.That(b.Value, Is.EqualTo 110)

