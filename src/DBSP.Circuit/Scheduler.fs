namespace DBSP.Circuit

open System
open System.Collections.Generic
open System.Runtime.CompilerServices

/// Scheduling priority for operators
type SchedulingPriority = 
    | Low = 0
    | Normal = 1  
    | High = 2
    | Critical = 3

/// Simple dependency scheduler for circuits
type DependencyScheduler internal (circuit: CircuitDefinition) =
    let topology = circuit.Dependencies
    let mutable executionOrder: NodeId list = []
    
    /// Compute topological sort order
    let computeTopologicalOrder() =
        let inDegree = Dictionary<NodeId, int>()
        let adjList = Dictionary<NodeId, NodeId list>()
        
        // Initialize
        for kvp in circuit.Operators do
            let nodeId = kvp.Key
            inDegree.[nodeId] <- 0
            adjList.[nodeId] <- []
        
        // Build adjacency list and compute in-degrees  
        for kvp in topology do
            let target = kvp.Key
            let dependencies = kvp.Value
            inDegree.[target] <- dependencies.Length
            
            for source in dependencies do
                match adjList.TryGetValue(source) with
                | true, neighbors -> adjList.[source] <- target :: neighbors
                | false, _ -> adjList.[source] <- [target]
        
        // Topological sort using Kahn's algorithm
        let queue = Queue<NodeId>()
        let result = List<NodeId>()
        
        // Start with nodes having no dependencies
        for kvp in inDegree do
            if kvp.Value = 0 then
                queue.Enqueue(kvp.Key)
        
        while queue.Count > 0 do
            let current = queue.Dequeue()
            result.Add(current)
            
            match adjList.TryGetValue(current) with
            | true, neighbors ->
                for neighbor in neighbors do
                    inDegree.[neighbor] <- inDegree.[neighbor] - 1
                    if inDegree.[neighbor] = 0 then
                        queue.Enqueue(neighbor)
            | false, _ -> ()
        
        executionOrder <- result |> List.ofSeq
    
    do computeTopologicalOrder()
    
    /// Get execution order
    member _.ExecutionOrder = executionOrder

module SchedulerModule =
    /// Create dependency scheduler
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let createDependencyScheduler (circuit: CircuitDefinition) = 
        DependencyScheduler(circuit)