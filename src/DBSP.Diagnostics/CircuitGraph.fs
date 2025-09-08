/// Circuit graph representation for debugging and validation
/// Maintains a model of the circuit topology built from CircuitEvents
module DBSP.Diagnostics.CircuitGraph

open System
open System.Collections.Generic
open System.Collections.Concurrent
open DBSP.Diagnostics.CircuitEvents

/// Node information in the circuit graph
type CircuitNode = {
    NodeId: GlobalNodeId
    Name: string
    OperatorType: string
    Location: SourceLocation option
    IsIterative: bool
    Children: Set<GlobalNodeId>  
    Parents: Set<GlobalNodeId>
    Edges: Map<EdgeKind, Set<GlobalNodeId>>
}

/// Circuit information
type CircuitInfo = {
    CircuitId: int64
    IsIterative: bool
    Nodes: Set<GlobalNodeId>
    CompletionStatus: bool
}

/// Region information for hierarchical grouping
type RegionInfo = {
    Name: string
    Location: SourceLocation option
    Nodes: ResizeArray<GlobalNodeId>
    StartTime: DateTime
}

/// Circuit graph maintains topology and validation state
type CircuitGraph = {
    /// All nodes indexed by GlobalNodeId
    Nodes: ConcurrentDictionary<GlobalNodeId, CircuitNode>
    /// Circuit information indexed by circuit ID
    Circuits: ConcurrentDictionary<int64, CircuitInfo>
    /// Current region stack for hierarchical construction
    RegionStack: Stack<RegionInfo>
    /// Validation errors encountered
    mutable Errors: TraceError list
    /// Statistics about the graph
    mutable Stats: GraphStats
}

and GraphStats = {
    NodeCount: int
    EdgeCount: int
    CircuitCount: int
    RegionDepth: int
    LastUpdated: DateTime
}

/// Circuit graph operations
module CircuitGraph =
    
    /// Create empty circuit graph
    let create () = {
        Nodes = ConcurrentDictionary<GlobalNodeId, CircuitNode>()
        Circuits = ConcurrentDictionary<int64, CircuitInfo>()
        RegionStack = Stack<RegionInfo>()
        Errors = []
        Stats = { NodeCount = 0; EdgeCount = 0; CircuitCount = 0; RegionDepth = 0; LastUpdated = DateTime.UtcNow }
    }
    
    /// Add a node to the graph
    let addNode (graph: CircuitGraph) (nodeId: GlobalNodeId) (name: string) (operatorType: string) (location: SourceLocation option) =
        let node = {
            NodeId = nodeId
            Name = name
            OperatorType = operatorType
            Location = location
            IsIterative = false
            Children = Set.empty
            Parents = Set.empty
            Edges = Map.empty
        }
        
        if graph.Nodes.TryAdd(nodeId, node) then
            graph.Stats <- { graph.Stats with NodeCount = graph.Stats.NodeCount + 1; LastUpdated = DateTime.UtcNow }
            Ok ()
        else
            let error = TraceError.NodeExists(nodeId)
            graph.Errors <- error :: graph.Errors
            Error error
    
    /// Add a circuit to the graph
    let addCircuit (graph: CircuitGraph) (circuitId: int64) (isIterative: bool) =
        let circuitInfo = {
            CircuitId = circuitId
            IsIterative = isIterative
            Nodes = Set.empty
            CompletionStatus = false
        }
        
        if graph.Circuits.TryAdd(circuitId, circuitInfo) then
            graph.Stats <- { graph.Stats with CircuitCount = graph.Stats.CircuitCount + 1; LastUpdated = DateTime.UtcNow }
            Ok ()
        else
            let error = TraceError.InvalidEvent($"Circuit {circuitId} already exists")
            graph.Errors <- error :: graph.Errors
            Error error
    
    /// Add an edge between two nodes
    let addEdge (graph: CircuitGraph) (kind: EdgeKind) (from: GlobalNodeId) (target: GlobalNodeId) =
        match graph.Nodes.TryGetValue(from), graph.Nodes.TryGetValue(target) with
        | (true, fromNode), (true, targetNode) ->
            // Check for duplicate edge
            let existingEdges = fromNode.Edges.TryFind(kind) |> Option.defaultValue Set.empty
            if Set.contains target existingEdges then
                let error = TraceError.DuplicateEdge(kind, from, target)
                graph.Errors <- error :: graph.Errors
                Error error
            else
                // Update from node edges and children
                let updatedFromEdges = fromNode.Edges.Add(kind, Set.add target existingEdges)
                let updatedFromNode = { 
                    fromNode with 
                        Edges = updatedFromEdges
                        Children = Set.add target fromNode.Children 
                }
                
                // Update target node parents
                let updatedTargetNode = { targetNode with Parents = Set.add from targetNode.Parents }
                
                // Update both nodes in the dictionary
                graph.Nodes.[from] <- updatedFromNode
                graph.Nodes.[target] <- updatedTargetNode
                
                graph.Stats <- { graph.Stats with EdgeCount = graph.Stats.EdgeCount + 1; LastUpdated = DateTime.UtcNow }
                Ok ()
                
        | (false, _), _ ->
            let error = TraceError.UnknownNode(from)
            graph.Errors <- error :: graph.Errors
            Error error
            
        | _, (false, _) ->
            let error = TraceError.UnknownNode(target)
            graph.Errors <- error :: graph.Errors
            Error error
    
    /// Push a region onto the stack
    let pushRegion (graph: CircuitGraph) (name: string) (location: SourceLocation option) =
        let region = {
            Name = name
            Location = location
            Nodes = ResizeArray<GlobalNodeId>()
            StartTime = DateTime.UtcNow
        }
        graph.RegionStack.Push(region)
        let newDepth = graph.RegionStack.Count
        graph.Stats <- { graph.Stats with RegionDepth = max graph.Stats.RegionDepth newDepth; LastUpdated = DateTime.UtcNow }
        Ok ()
    
    /// Pop a region from the stack
    let popRegion (graph: CircuitGraph) =
        if graph.RegionStack.Count > 0 then
            let region = graph.RegionStack.Pop()
            Ok region
        else
            let error = TraceError.RegionStackError("Cannot pop from empty region stack")
            graph.Errors <- error :: graph.Errors
            Error error
    
    /// Add node to current region
    let addToCurrentRegion (graph: CircuitGraph) (nodeId: GlobalNodeId) =
        if graph.RegionStack.Count > 0 then
            let currentRegion = graph.RegionStack.Peek()
            currentRegion.Nodes.Add(nodeId)
            Ok ()
        else
            Ok () // No current region is fine
    
    /// Get node by ID
    let tryGetNode (graph: CircuitGraph) (nodeId: GlobalNodeId) =
        match graph.Nodes.TryGetValue(nodeId) with
        | true, node -> Some node
        | false, _ -> None
    
    /// Get circuit by ID  
    let tryGetCircuit (graph: CircuitGraph) (circuitId: int64) =
        match graph.Circuits.TryGetValue(circuitId) with
        | true, circuit -> Some circuit
        | false, _ -> None
    
    /// Get all nodes in the graph
    let getAllNodes (graph: CircuitGraph) =
        graph.Nodes.Values |> Seq.toList
    
    /// Get all circuits in the graph
    let getAllCircuits (graph: CircuitGraph) =
        graph.Circuits.Values |> Seq.toList
    
    /// Get current region stack  
    let getCurrentRegionStack (graph: CircuitGraph) =
        graph.RegionStack.ToArray() |> Array.toList |> List.rev
    
    /// Validate graph consistency
    let validate (graph: CircuitGraph) =
        let mutable errors = []
        
        // Validate that all edge targets exist
        for node in graph.Nodes.Values do
            for edgeKind in node.Edges do
                for target in edgeKind.Value do
                    if not (graph.Nodes.ContainsKey(target)) then
                        errors <- TraceError.UnknownNode(target) :: errors
        
        // Validate parent-child consistency
        for node in graph.Nodes.Values do
            for child in node.Children do
                match graph.Nodes.TryGetValue(child) with
                | true, childNode ->
                    if not (Set.contains node.NodeId childNode.Parents) then
                        errors <- TraceError.InvalidEvent($"Inconsistent parent-child relationship: {node.NodeId} -> {child}") :: errors
                | false, _ ->
                    errors <- TraceError.UnknownNode(child) :: errors
        
        if List.isEmpty errors then Ok () else Error errors
    
    /// Get graph statistics
    let getStats (graph: CircuitGraph) = graph.Stats
    
    /// Clear all errors
    let clearErrors (graph: CircuitGraph) =
        graph.Errors <- []