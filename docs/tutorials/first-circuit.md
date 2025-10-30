# Building Your First Circuit

This tutorial shows how to assemble a tiny DBSP circuit that reacts to incoming
deltas, maintains state with an `IntegrateOperator`, and exposes that state via
stream handles. You will:

- instantiate a `CircuitBuilder`
- register stream handles for deltas, state, and a logical clock
- plug in a custom `IExecutable` that drives the integration step
- execute the circuit with `CircuitRuntimeModule.buildAndInit`

Reference implementation:
[`examples/DBSP.Tutorials/FirstCircuit.fs`](../../examples/DBSP.Tutorials/FirstCircuit.fs).

## Run the tutorial

```bash
dotnet run --project examples/DBSP.Tutorials -- --sample first-circuit
```

Example output:

```
step 1: state={apple:1; banana:2}
step 2: state={apple:2; banana:1; cherry:1}
step 3: state={banana:1; cherry:2}
```

## Key concepts

### 1. Wiring stream handles

```fsharp
let deltaStream = builder.AddInput<ZSet<string>>("fruit-delta")
let stateStream = builder.AddInput<ZSet<string>>("fruit-state")
let _ = builder.AddOutput(stateStream, "fruit-state")
let clock = builder.AddClock("clock")
```

Stream handles are regular mutable records. The runtime exposes them so callers
can push new deltas (`deltaStream.Value <- Some delta`) and inspect the latest
state (`stateStream.Value`).

### 2. Executable operator

```fsharp
type IntegrateExecutable(delta: StreamHandle<ZSet<string>>, state: StreamHandle<ZSet<string>>) =
    let integrate = IntegrateOperator<string>()
    interface IExecutable with
        member _.StepAsync() = task {
            match delta.Value with
            | Some d ->
                let! next = integrate.EvalAsync d
                state.Value <- Some next
                delta.Value <- None
            | None -> ()
        }
```

Implement `IExecutable.StepAsync` to perform the per-step work. After consuming
the delta the executable clears the handle, mimicking a single-use input queue.

### 3. Running the circuit

```fsharp
let handle, handles = startCircuit()
handle.Start() |> ignore
handle.Runtime.ExecuteStepAsync() |> Async.AwaitTask |> Async.RunSynchronously |> ignore
```

`handles.delta` and `handles.clock` are set before each call to `StepAsync`.
The tutorial feeds three scripted delta batches to demonstrate how the state
evolves without recomputing from scratch.

## Where to go next

- Deepen your understanding of the algebra by completing
  [Understanding Z-Sets](understanding-zsets.md).
- Explore multi-input operators in [Incremental Joins](incremental-joins.md).
