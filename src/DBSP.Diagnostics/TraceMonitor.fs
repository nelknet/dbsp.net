/// TraceMonitor equivalent - circuit debugging and validation infrastructure
/// Based on Feldera's TraceMonitor implementation patterns
module DBSP.Diagnostics.TraceMonitor

open System
open System.Collections.Concurrent
open System.IO
open System.Text
open DBSP.Diagnostics.CircuitEvents
open DBSP.Diagnostics.CircuitGraph
open DBSP.Diagnostics.StateValidation

/// Monitor configuration options
type MonitorConfig = {
    /// Enable circuit construction validation
    ValidateConstruction: bool
    /// Enable runtime state validation  
    ValidateRuntime: bool
    /// Enable GraphViz visualization generation
    EnableVisualization: bool
    /// Panic on validation errors (throw exceptions)
    PanicOnError: bool
    /// Maximum number of errors to collect before stopping
    MaxErrors: int option
    /// Log validation errors to console
    LogErrors: bool
}

/// Default monitor configuration
module MonitorConfig =
    let defaultConfig = {
        ValidateConstruction = true
        ValidateRuntime = true
        EnableVisualization = false
        PanicOnError = false
        MaxErrors = Some 100
        LogErrors = true
    }
    
    let panicOnErrorConfig = {
        defaultConfig with PanicOnError = true
    }

/// Internal implementation of TraceMonitor
type TraceMonitorInternal = {
    /// Configuration
    Config: MonitorConfig
    /// Circuit graph for construction validation
    Graph: CircuitGraph
    /// State validator for runtime validation
    StateValidator: StateValidator
    /// Circuit event handler
    CircuitEventHandler: EventHandler<CircuitEvent> option
    /// Scheduler event handler  
    SchedulerEventHandler: EventHandler<SchedulerEvent> option
    /// Handler name for identification
    HandlerName: string
    /// Creation timestamp
    CreatedAt: DateTime
    /// Statistics
    mutable TotalCircuitEvents: int64
    mutable TotalSchedulerEvents: int64
    mutable TotalErrors: int64
}

/// Main TraceMonitor interface (equivalent to Rust's TraceMonitor)
type TraceMonitor = {
    Internal: TraceMonitorInternal
}

/// TraceMonitor operations
module TraceMonitor =
    
    /// Create new TraceMonitor with default configuration
    let create (config: MonitorConfig) (handlerName: string) = {
        Internal = {
            Config = config
            Graph = CircuitGraph.create()
            StateValidator = StateValidator.create()
            CircuitEventHandler = None
            SchedulerEventHandler = None
            HandlerName = handlerName
            CreatedAt = DateTime.UtcNow
            TotalCircuitEvents = 0L
            TotalSchedulerEvents = 0L
            TotalErrors = 0L
        }
    }
    
    /// Create TraceMonitor that panics on errors (for testing)
    let createPanicOnError (handlerName: string) =
        create MonitorConfig.panicOnErrorConfig handlerName
    
    /// Process a circuit construction event
    let processCircuitEvent (monitor: TraceMonitor) (event: CircuitEvent) =
        let internalState = monitor.Internal
        internalState.TotalCircuitEvents <- internalState.TotalCircuitEvents + 1L
        
        if internalState.Config.ValidateConstruction then
            let result = 
                match event with
                | Operator(nodeId, name, location) ->
                    CircuitGraph.addNode internalState.Graph nodeId name "Operator" location
                    |> Result.bind (fun _ -> CircuitGraph.addToCurrentRegion internalState.Graph nodeId)
                
                | StrictOperatorOutput(nodeId, name, location) ->
                    CircuitGraph.addNode internalState.Graph nodeId name "StrictOperatorOutput" location
                    |> Result.bind (fun _ -> CircuitGraph.addToCurrentRegion internalState.Graph nodeId)
                
                | StrictOperatorInput(nodeId, outputNodeId) ->
                    CircuitGraph.addEdge internalState.Graph EdgeKind.Dependency outputNodeId nodeId
                
                | Subcircuit(nodeId, iterative) ->
                    CircuitGraph.addCircuit internalState.Graph nodeId.CircuitId iterative
                    |> Result.bind (fun _ -> CircuitGraph.addNode internalState.Graph nodeId "Subcircuit" "Subcircuit" None)
                
                | SubcircuitComplete(nodeId) ->
                    // Mark circuit as complete
                    Ok ()
                
                | Edge(kind, from, target) ->
                    CircuitGraph.addEdge internalState.Graph kind from target
                
                | PushRegion(name, location) ->
                    CircuitGraph.pushRegion internalState.Graph name location
                
                | PopRegion ->
                    CircuitGraph.popRegion internalState.Graph |> Result.map ignore
            
            match result with
            | Ok _ -> ()
            | Error error ->
                internalState.TotalErrors <- internalState.TotalErrors + 1L
                
                if internalState.Config.LogErrors then
                    printfn "[TraceMonitor:%s] Circuit event error: %A for event: %A" internalState.HandlerName error event
                
                if internalState.Config.PanicOnError then
                    failwithf "TraceMonitor validation failed: %A" error
                
                // Call custom handler if provided
                internalState.CircuitEventHandler
                |> Option.iter (fun handler -> handler event error)
                
                // Check if we should stop due to too many errors
                match internalState.Config.MaxErrors with
                | Some maxErrors when internalState.TotalErrors >= int64 maxErrors ->
                    failwithf "TraceMonitor exceeded maximum error count (%d)" maxErrors
                | _ -> ()
    
    /// Process a scheduler execution event
    let processSchedulerEvent (monitor: TraceMonitor) (event: SchedulerEvent) =
        let internalState = monitor.Internal
        internalState.TotalSchedulerEvents <- internalState.TotalSchedulerEvents + 1L
        
        if internalState.Config.ValidateRuntime then
            let result = StateValidator.processEvent internalState.StateValidator event
            
            match result with
            | Ok _ -> ()
            | Error error ->
                internalState.TotalErrors <- internalState.TotalErrors + 1L
                
                if internalState.Config.LogErrors then
                    printfn "[TraceMonitor:%s] Scheduler event error: %A for event: %A" internalState.HandlerName error event
                
                if internalState.Config.PanicOnError then
                    failwithf "TraceMonitor validation failed: %A" error
                
                // Call custom handler if provided
                internalState.SchedulerEventHandler
                |> Option.iter (fun handler -> handler event error)
    
    /// Set expected nodes for validation
    let setExpectedNodes (monitor: TraceMonitor) (circuitId: int64) (nodes: Set<GlobalNodeId>) =
        StateValidator.setExpectedNodes monitor.Internal.StateValidator circuitId nodes
    
    /// Generate GraphViz visualization of the circuit
    let generateVisualization (monitor: TraceMonitor) =
        if not monitor.Internal.Config.EnableVisualization then
            failwith "Visualization not enabled in monitor configuration"
        
        let sb = StringBuilder()
        sb.AppendLine("digraph Circuit {") |> ignore
        sb.AppendLine("  rankdir=TB;") |> ignore
        sb.AppendLine("  node [shape=box];") |> ignore
        
        // Add nodes
        for node in CircuitGraph.getAllNodes monitor.Internal.Graph do
            let label = sprintf "%s\\n%s" node.Name node.OperatorType
            let location = 
                match node.Location with
                | Some loc -> sprintf "\\n%s:%d" (Path.GetFileName(loc.File)) loc.Line
                | None -> ""
            
            sb.AppendLine(sprintf "  \"%A\" [label=\"%s%s\"];" node.NodeId label location) |> ignore
        
        // Add edges
        for node in CircuitGraph.getAllNodes monitor.Internal.Graph do
            for edgeKind in node.Edges do
                let edgeStyle = 
                    match edgeKind.Key with
                    | Stream -> ""
                    | Dependency -> " [style=dashed]"
                    | Feedback -> " [color=red]"
                
                for target in edgeKind.Value do
                    sb.AppendLine(sprintf "  \"%A\" -> \"%A\"%s;" node.NodeId target edgeStyle) |> ignore
        
        // Add regions as clusters
        let regions = CircuitGraph.getCurrentRegionStack monitor.Internal.Graph
        for i, region in List.indexed regions do
            sb.AppendLine(sprintf "  subgraph cluster_%d {" i) |> ignore
            sb.AppendLine(sprintf "    label=\"%s\";" region.Name) |> ignore
            sb.AppendLine("    style=dashed;") |> ignore
            
            for nodeId in region.Nodes do
                sb.AppendLine(sprintf "    \"%A\";" nodeId) |> ignore
            
            sb.AppendLine("  }") |> ignore
        
        sb.AppendLine("}") |> ignore
        sb.ToString()
    
    /// Get monitor statistics
    let getStats (monitor: TraceMonitor) =
        let graphStats = CircuitGraph.getStats monitor.Internal.Graph
        {|
            HandlerName = monitor.Internal.HandlerName
            CreatedAt = monitor.Internal.CreatedAt
            TotalCircuitEvents = monitor.Internal.TotalCircuitEvents
            TotalSchedulerEvents = monitor.Internal.TotalSchedulerEvents
            TotalErrors = monitor.Internal.TotalErrors
            GraphStats = graphStats
            StateValidatorContexts = StateValidator.getAllContexts monitor.Internal.StateValidator |> List.length
        |}
    
    /// Get all validation errors
    let getAllErrors (monitor: TraceMonitor) =
        let graphErrors = monitor.Internal.Graph.Errors
        let stateErrors = StateValidator.getErrors monitor.Internal.StateValidator
        graphErrors @ stateErrors
    
    /// Clear all validation errors
    let clearErrors (monitor: TraceMonitor) =
        CircuitGraph.clearErrors monitor.Internal.Graph
        StateValidator.clearErrors monitor.Internal.StateValidator
    
    /// Validate final state (call at end of execution)
    let validateFinalState (monitor: TraceMonitor) =
        let graphValidation = CircuitGraph.validate monitor.Internal.Graph
        let stateValidation = StateValidator.validateFinalState monitor.Internal.StateValidator
        
        match graphValidation, stateValidation with
        | Ok _, Ok _ -> Ok ()
        | Error graphErrors, Ok _ -> Error graphErrors
        | Ok _, Error stateErrors -> Error stateErrors
        | Error graphErrors, Error stateErrors -> Error (graphErrors @ stateErrors)
    
    /// Save visualization to file
    let saveVisualization (monitor: TraceMonitor) (filePath: string) =
        let dot = generateVisualization monitor
        File.WriteAllText(filePath, dot)

/// Extension methods for easier circuit integration  
type TraceMonitorExtensions() =
    
    /// Attach monitor to receive circuit events
    [<System.Runtime.CompilerServices.Extension>]
    static member AttachCircuitEvents(monitor: TraceMonitor, eventSource: IObservable<CircuitEvent>) =
        eventSource.Subscribe(fun event -> TraceMonitor.processCircuitEvent monitor event)
    
    /// Attach monitor to receive scheduler events
    [<System.Runtime.CompilerServices.Extension>] 
    static member AttachSchedulerEvents(monitor: TraceMonitor, eventSource: IObservable<SchedulerEvent>) =
        eventSource.Subscribe(fun event -> TraceMonitor.processSchedulerEvent monitor event)