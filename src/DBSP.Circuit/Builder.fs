namespace DBSP.Circuit

open System
open System.Collections.Generic
open System.Runtime.CompilerServices
open System.Threading.Tasks
open FSharp.Data.Adaptive

/// Node identifier for circuit topology
[<Struct>]
type NodeId = 
    { Id: int64 }
    member this.Value = this.Id
    static member Create(id: int64) = { Id = id }
    override this.ToString() = $"Node({this.Id})"

/// Global node identifier across nested circuits
[<Struct>]
type GlobalNodeId =
    { CircuitId: int64; NodeId: NodeId }
    member this.AsString = $"Circuit({this.CircuitId}).{this.NodeId}"
    override this.ToString() = this.AsString

/// Operator metadata for debugging and monitoring
[<Struct>]
type OperatorMetadata = {
    Name: string
    TypeInfo: string
    Location: string option
}

/// Ownership preference for stream data transfer optimization
[<Struct>]
type OwnershipPreference = 
    | PreferRef
    | PreferOwned  
    | NoPreference

/// Circuit scope for nested circuit management
type Scope =
    | RootScope
    | ChildScope of parent: obj

/// Stream handle representing data flow between operators
[<Struct>]
type StreamHandle<'T> = {
    NodeId: NodeId
    mutable Value: 'T option
    mutable Consumers: int
    mutable RemainingConsumers: int
}

/// Simple circuit builder with basic functionality
type CircuitBuilder internal (circuitId: int64, scope: Scope) =
    let mutable nextNodeId = 0L
    let operators = Dictionary<NodeId, obj>()
    let connections = List<NodeId * NodeId>()
    let inputHandles = Dictionary<string, obj>() 
    let outputHandles = Dictionary<string, obj>()
    let dependencies = Dictionary<NodeId, NodeId list>()
    
    /// Generate unique node identifier
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let generateNodeId() =
        let id = System.Threading.Interlocked.Increment(&nextNodeId)
        NodeId.Create(id - 1L)
    
    /// Add dependency relationship between nodes
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let addDependency sourceNode targetNode =
        match dependencies.TryGetValue(targetNode) with
        | true, deps -> dependencies.[targetNode] <- sourceNode :: deps
        | false, _ -> dependencies.[targetNode] <- [sourceNode]
    
    /// Add operator to circuit
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    member this.AddOperator<'Op>(op: 'Op, metadata: OperatorMetadata) : NodeId =
        let nodeId = generateNodeId()
        operators.[nodeId] <- box op
        nodeId
    
    /// Connect two nodes with dependency relationship
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    member this.ConnectNodes(source: NodeId, target: NodeId) : unit =
        connections.Add((source, target))
        addDependency source target
    
    /// Create input stream with specified name and type
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    member this.AddInput<'T>(name: string) : StreamHandle<'T> =
        let nodeId = generateNodeId()
        let streamHandle = {
            NodeId = nodeId
            Value = None
            Consumers = 0
            RemainingConsumers = 0
        }
        inputHandles.[name] <- box streamHandle
        streamHandle
    
    /// Create output stream from source with specified name
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    member this.AddOutput<'T>(source: StreamHandle<'T>, name: string) : StreamHandle<'T> =
        outputHandles.[name] <- box source
        source
    
    /// Build final circuit
    member this.Build() =
        {|
            CircuitId = circuitId
            Operators = operators |> Seq.map (fun kvp -> kvp.Key, kvp.Value) |> Map.ofSeq
            Connections = connections |> List.ofSeq
            InputHandles = inputHandles |> Seq.map (fun kvp -> kvp.Key, kvp.Value) |> Map.ofSeq  
            OutputHandles = outputHandles |> Seq.map (fun kvp -> kvp.Key, kvp.Value) |> Map.ofSeq
            Dependencies = dependencies |> Seq.map (fun kvp -> kvp.Key, kvp.Value) |> Map.ofSeq
            Scope = scope
        |}

/// Built circuit containing all operators and connections
type CircuitDefinition = {|
    CircuitId: int64
    Operators: Map<NodeId, obj>
    Connections: (NodeId * NodeId) list
    InputHandles: Map<string, obj>
    OutputHandles: Map<string, obj>
    Dependencies: Map<NodeId, NodeId list>
    Scope: Scope
|}

/// Root circuit builder for creating top-level circuits
type RootCircuit private () =
    static let mutable nextCircuitId = 0L
    
    /// Generate unique circuit identifier
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    static let generateCircuitId() =
        System.Threading.Interlocked.Increment(&nextCircuitId) - 1L
    
    /// Build root circuit using construction callback
    static member Build<'TResult>(constructFn: CircuitBuilder -> 'TResult) : CircuitDefinition * 'TResult =
        let circuitId = generateCircuitId()
        let builder = CircuitBuilder(circuitId, RootScope)
        let result = constructFn builder
        let circuit = builder.Build()
        (circuit, result)

module CircuitBuilderModule =
    /// Create new root circuit builder
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let create() = CircuitBuilder(0L, RootScope)
    
    /// Build circuit with construction function
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let build<'T> (constructFn: CircuitBuilder -> 'T) : CircuitDefinition * 'T =
        RootCircuit.Build(constructFn)