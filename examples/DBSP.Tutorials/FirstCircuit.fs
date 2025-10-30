module DBSP.Tutorials.FirstCircuit

open System.Threading.Tasks
open DBSP.Core.ZSet
open DBSP.Circuit
open DBSP.Tutorials.Common
open DBSP.Operators.TemporalOperators

type FruitCircuitHandles = {
    Delta: StreamHandle<ZSet<string>>
    State: StreamHandle<ZSet<string>>
    Clock: StreamHandle<int64>
}

type IntegrateExecutable(delta: StreamHandle<ZSet<string>>, state: StreamHandle<ZSet<string>>) =
    let integrate = new IntegrateOperator<string>()
    interface IExecutable with
        member _.StepAsync() =
            task {
                match delta.Value with
                | Some d ->
                    let! snapshot = integrate.EvalAsyncImpl d
                    state.Value <- Some snapshot
                    delta.Value <- None
                | None -> ()
            }

let private mkDelta (pairs: (string * int) list) =
    ZSet.buildWith (fun builder ->
        for (key, weight) in pairs do
            builder.Add(key, weight))

let private scriptedDeltas =
    [ [ "apple", 1; "banana", 2 ]
      [ "apple", 1; "banana", -1; "cherry", 1 ]
      [ "apple", -1; "cherry", 1 ] ]

let private startCircuit () =
    let config = RuntimeConfig.Default
    match CircuitRuntimeModule.buildAndInit (fun builder ->
              let delta = builder.AddInput<ZSet<string>>("fruit-delta")
              let state = builder.AddInput<ZSet<string>>("fruit-state")
              let _ = builder.AddOutput(state, "fruit-state")
              let clock = builder.AddClock("clock")
              builder.AddExecutable(IntegrateExecutable(delta, state))
              { Delta = delta; State = state; Clock = clock }) config with
    | Ok ((handle: CircuitHandle), (handles: FruitCircuitHandles)) -> handle, handles
    | Error err -> failwithf "Failed to build circuit: %s" err

let run () =
    printHeader "First Circuit — Integrate via runtime"
    let circuitHandle, handles = startCircuit ()
    let _ = circuitHandle.Start()

    scriptedDeltas
    |> List.iteri (fun idx pairs ->
        handles.Delta.Value <- Some (mkDelta pairs)
        match circuitHandle.Step() with
        | Ok () ->
            let state =
                handles.State.Value
                |> Option.defaultValue (ZSet.empty<string>)
            let tick = handles.Clock.Value |> Option.defaultValue 0L
            printStep (int tick) "state" (formatZSet state id)
        | Error err -> failwithf "Circuit step failed: %s" err)
