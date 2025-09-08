namespace DBSP.Circuit

open System
open System.Collections.Generic
open System.Runtime.CompilerServices

/// Optimization rule interface
type IOptimizationRule =
    abstract member Name: string
    abstract member CanApply: circuit: CircuitDefinition -> bool
    abstract member Apply: circuit: CircuitDefinition -> CircuitDefinition

/// Dead code elimination rule
type DeadCodeEliminationRule() =
    /// Find operators with no downstream consumers
    let findDeadOperators (circuit: CircuitDefinition) : NodeId list =
        let hasOutput nodeId = 
            circuit.Connections 
            |> List.exists (fun (source, _) -> source = nodeId) ||
            circuit.OutputHandles.Values 
            |> Seq.exists (fun _ -> false) // Simplified for now
        
        circuit.Operators.Keys
        |> Seq.filter (not << hasOutput)
        |> List.ofSeq
    
    interface IOptimizationRule with
        member _.Name = "Dead Code Elimination"
        
        member _.CanApply(circuit: CircuitDefinition) =
            not (findDeadOperators circuit).IsEmpty
        
        member _.Apply(circuit: CircuitDefinition) =
            let deadOperators = findDeadOperators circuit
            let optimizedOperators = 
                deadOperators |> List.fold (fun ops nodeId -> Map.remove nodeId ops) circuit.Operators
            
            let optimizedConnections = 
                circuit.Connections
                |> List.filter (fun (source, target) -> 
                    not (List.contains source deadOperators) && not (List.contains target deadOperators))
            
            {| circuit with
                Operators = optimizedOperators
                Connections = optimizedConnections
            |}

/// Simple circuit optimizer
type CircuitOptimizer internal (rules: IOptimizationRule list) =
    /// Apply optimization rules once
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    member this.OptimizeOnce(circuit: CircuitDefinition) : CircuitDefinition =
        let mutable optimizedCircuit = circuit
        for rule in rules do
            if rule.CanApply(optimizedCircuit) then
                optimizedCircuit <- rule.Apply(optimizedCircuit)
        optimizedCircuit

module CircuitOptimizerModule =
    /// Standard optimization rules
    let standardRules : IOptimizationRule list = [
        DeadCodeEliminationRule() :> IOptimizationRule
    ]
    
    /// Create optimizer with standard rules
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let createStandard() = CircuitOptimizer(standardRules)
    
    /// Optimize circuit with standard optimizations
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let optimize (circuit: CircuitDefinition) : CircuitDefinition =
        let optimizer = createStandard()
        optimizer.OptimizeOnce(circuit)