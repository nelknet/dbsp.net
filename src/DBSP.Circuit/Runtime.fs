namespace DBSP.Circuit

open System
open System.Collections.Generic
open System.Runtime.CompilerServices
open System.Threading
open System.Threading.Tasks

/// Runtime configuration for circuit execution
type RuntimeConfig = {
    WorkerThreads: int
    StepIntervalMs: int
    MaxBufferSize: int
    EnableCheckpointing: bool
    StoragePath: string option
} with
    static member Default = {
        WorkerThreads = Environment.ProcessorCount
        StepIntervalMs = 100
        MaxBufferSize = 10000
        EnableCheckpointing = false
        StoragePath = None
    }

/// Circuit execution state
type CircuitState = 
    | Created
    | Running
    | Paused
    | Terminated
    | Failed of error: exn

/// Simple circuit runtime with basic execution
type CircuitRuntime internal (circuit: CircuitDefinition, config: RuntimeConfig) =
    let mutable currentState = Created
    let mutable stepsExecuted = 0L
    
    /// Execute single circuit step
    member this.ExecuteStepAsync() = task {
        if currentState = Running then
            try
                // Basic execution - increment step counter
                stepsExecuted <- stepsExecuted + 1L
                return Ok ()
            with
            | ex ->
                currentState <- Failed ex
                return Error ex.Message
        else
            return Error "Circuit is not running"
    }
    
    /// Start circuit execution
    member this.Start() = 
        currentState <- Running
        Ok ()
    
    /// Pause circuit execution
    member this.Pause() = 
        currentState <- Paused
        Ok ()
    
    /// Terminate circuit execution
    member this.Terminate() =
        currentState <- Terminated
        Ok ()
    
    /// Get current circuit state
    member this.State = currentState
    
    /// Get steps executed
    member this.StepsExecuted = stepsExecuted

/// Circuit handle for managing circuit execution
type CircuitHandle = {
    Runtime: CircuitRuntime
    InputHandles: Map<string, obj>
    OutputHandles: Map<string, obj>
    Config: RuntimeConfig
} with
    /// Execute single step
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    member this.Step() = 
        this.Runtime.ExecuteStepAsync() |> Async.AwaitTask |> Async.RunSynchronously
    
    /// Start execution
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    member this.Start() = this.Runtime.Start()
    
    /// Get steps executed
    member this.StepsExecuted = this.Runtime.StepsExecuted

module CircuitRuntimeModule =
    /// Create circuit runtime with configuration
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let create (circuit: CircuitDefinition) (config: RuntimeConfig) : CircuitRuntime =
        new CircuitRuntime(circuit, config)
    
    /// Build and initialize circuit with runtime
    let buildAndInit<'T> (constructFn: CircuitBuilder -> 'T) (config: RuntimeConfig) =
        try
            let (circuit, result) = RootCircuit.Build(constructFn)
            let runtime = create circuit config
            let handle = {
                Runtime = runtime
                InputHandles = circuit.InputHandles
                OutputHandles = circuit.OutputHandles
                Config = config
            }
            Ok (handle, result)
        with
        | ex -> Error ex.Message