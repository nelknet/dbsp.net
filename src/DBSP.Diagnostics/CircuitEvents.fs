/// Circuit events for debugging and validation
/// Based on Feldera's TraceMonitor event system
module DBSP.Diagnostics.CircuitEvents

open System
open System.Runtime.CompilerServices

/// Global node identifier for circuit elements
[<Struct>]
type GlobalNodeId = 
    { CircuitId: int64; NodeId: int32 }
    
    static member Create(circuitId: int64, nodeId: int32) = 
        { CircuitId = circuitId; NodeId = nodeId }

/// Source location information for operators
[<Struct>]
type SourceLocation = {
    File: string
    Line: int
    Column: int
} with
    static member Create(file: string, line: int) =
        { File = file; Line = line; Column = 0 }

/// Types of edges between circuit nodes
type EdgeKind =
    | Stream        // Data stream connection
    | Dependency    // Control dependency
    | Feedback      // Recursive connection

/// Circuit construction events - track how circuit is built
type CircuitEvent =
    /// Regular operator creation
    | Operator of nodeId: GlobalNodeId * name: string * location: SourceLocation option
    /// Strict operator output creation (two-phase construction)
    | StrictOperatorOutput of nodeId: GlobalNodeId * name: string * location: SourceLocation option
    /// Strict operator input connection
    | StrictOperatorInput of nodeId: GlobalNodeId * outputNodeId: GlobalNodeId
    /// Subcircuit creation
    | Subcircuit of nodeId: GlobalNodeId * iterative: bool
    /// Subcircuit completion
    | SubcircuitComplete of nodeId: GlobalNodeId
    /// Edge creation between nodes
    | Edge of kind: EdgeKind * from: GlobalNodeId * target: GlobalNodeId
    /// Region start for hierarchical grouping
    | PushRegion of name: string * location: SourceLocation option
    /// Region end
    | PopRegion

/// Scheduler execution events - track runtime behavior  
type SchedulerEvent =
    /// Operator evaluation start
    | EvalStart of node: GlobalNodeId
    /// Operator evaluation end
    | EvalEnd of node: GlobalNodeId
    /// Circuit waiting start
    | WaitStart of circuitId: int64
    /// Circuit waiting end
    | WaitEnd of circuitId: int64
    /// Circuit step start
    | StepStart of circuitId: int64
    /// Circuit step end
    | StepEnd of circuitId: int64  
    /// Clock cycle start
    | ClockStart
    /// Clock cycle end
    | ClockEnd

/// Validation errors that can occur
type TraceError =
    | UnknownNode of GlobalNodeId
    | NodeExists of GlobalNodeId
    | InvalidEvent of string
    | CircuitNotFound of int64
    | InvalidStateTransition of fromState: string * toState: string * event: SchedulerEvent
    | MissingParentCircuit of nodeId: GlobalNodeId
    | DuplicateEdge of EdgeKind * from: GlobalNodeId * target: GlobalNodeId
    | RegionStackError of string

/// Event handlers for validation errors
type EventHandler<'Event> = 'Event -> TraceError -> unit

/// Extension methods for creating source locations
[<Extension>]
type SourceLocationExtensions =
    
    [<Extension>]
    static member GetCurrentLocation(file: string, line: int) =
        SourceLocation.Create(file, line)

/// Helper functions for event creation
module Events =
    
    /// Create operator event with current source location
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let inline createOperator nodeId name (file: string) (line: int) =
        CircuitEvent.Operator(nodeId, name, Some (SourceLocation.Create(file, line)))
    
    /// Create edge event
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let inline createEdge kind from target =
        CircuitEvent.Edge(kind, from, target)
    
    /// Create region push event with source location
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let inline pushRegion name (file: string) (line: int) =
        CircuitEvent.PushRegion(name, Some (SourceLocation.Create(file, line)))

/// Pattern matching helpers for events
module EventPatterns =
    
    /// Active pattern for operator events
    let (|OperatorEvent|_|) = function
        | CircuitEvent.Operator(nodeId, name, location) -> Some(nodeId, name, location)
        | CircuitEvent.StrictOperatorOutput(nodeId, name, location) -> Some(nodeId, name, location)
        | _ -> None
    
    /// Active pattern for structural events
    let (|StructuralEvent|_|) = function
        | CircuitEvent.Subcircuit(nodeId, iterative) -> Some("Subcircuit", nodeId.ToString())
        | CircuitEvent.Edge(kind, from, target) -> Some("Edge", $"{from} -> {target} ({kind})")
        | CircuitEvent.PushRegion(name, _) -> Some("PushRegion", name)
        | CircuitEvent.PopRegion -> Some("PopRegion", "")
        | _ -> None