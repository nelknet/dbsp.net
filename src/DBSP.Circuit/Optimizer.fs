namespace DBSP.Circuit

open System
open System.Collections.Generic
open System.Runtime.CompilerServices
open DBSP.Operators
open DBSP.Operators.LinearOperators
open DBSP.Operators.FusedOperators

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
    /// Options to guard optional optimizations
    type OptimizerOptions = { EnableFusion: bool }

    /// Standard optimization rules
    let standardRules : IOptimizationRule list = [
        DeadCodeEliminationRule() :> IOptimizationRule
    ]
    
    /// Fusion rule: ZSetFilter -> ZSetMap => FilterMap
    type FilterMapFusionRule() =
        let isSingleProducerTarget (conn: (NodeId*NodeId) list) (target: NodeId) =
            conn |> List.filter (fun (_,t) -> t = target) |> List.length = 1
        let isSingleConsumerSource (conn: (NodeId*NodeId) list) (source: NodeId) =
            conn |> List.filter (fun (s,_) -> s = source) |> List.length = 1
        interface IOptimizationRule with
            member _.Name = "FilterMap Fusion"
            member _.CanApply(circuit) =
                circuit.Connections |> List.exists (fun (a,b) ->
                    match Map.tryFind a circuit.Operators, Map.tryFind b circuit.Operators with
                    | Some opA, Some opB ->
                        let aIsFilter = opA.GetType().Name.StartsWith("ZSetFilterOperator")
                        let bIsMap = opB.GetType().Name.StartsWith("ZSetMapOperator")
                        aIsFilter && bIsMap
                        && isSingleProducerTarget circuit.Connections b
                        && isSingleConsumerSource circuit.Connections a
                    | _ -> false)
            member _.Apply(circuit) =
                // Try to fuse the first matching pair
                let tryFuse (a:NodeId,b:NodeId) =
                    match Map.tryFind a circuit.Operators, Map.tryFind b circuit.Operators with
                    | Some opA, Some opB ->
                        let aIsFilter = opA.GetType().Name.StartsWith("ZSetFilterOperator")
                        let bIsMap = opB.GetType().Name.StartsWith("ZSetMapOperator")
                        if aIsFilter && bIsMap then
                            let predObj = opA.GetType().GetProperty("Predicate").GetValue(opA)
                            let mapObj = opB.GetType().GetProperty("Transform").GetValue(opB)
                            let mapType = opB.GetType()
                            let genArgs = mapType.GetGenericArguments()
                            let inTy = genArgs.[0]
                            let outTy = genArgs.[1]
                            let fusedTy = typeof<FilterMapOperator<_,_>>.GetGenericTypeDefinition().MakeGenericType([| inTy; outTy |])
                            let fused = Activator.CreateInstance(fusedTy, [| predObj; mapObj |])
                            let ops' = circuit.Operators |> Map.remove a |> Map.add b fused
                            let conns' =
                                circuit.Connections
                                |> List.collect (fun (s,t) ->
                                    if s = a && t = b then []
                                    elif t = a then [ (s, b) ]
                                    elif s = a then []
                                    else [ (s,t) ])
                            Some {|
                                circuit with
                                    Operators = ops'
                                    Connections = conns'
                            |}
                        else None
                    | _ -> None
                match circuit.Connections |> List.tryPick tryFuse with
                | Some c -> c
                | None -> circuit
    
    /// Fusion rule: ZSetMap -> ZSetFilter => MapFilter
    type MapFilterFusionRule() =
        let isSingleProducerTarget (conn: (NodeId*NodeId) list) (target: NodeId) =
            conn |> List.filter (fun (_,t) -> t = target) |> List.length = 1
        let isSingleConsumerSource (conn: (NodeId*NodeId) list) (source: NodeId) =
            conn |> List.filter (fun (s,_) -> s = source) |> List.length = 1
        interface IOptimizationRule with
            member _.Name = "MapFilter Fusion"
            member _.CanApply(circuit) =
                circuit.Connections |> List.exists (fun (a,b) ->
                    match Map.tryFind a circuit.Operators, Map.tryFind b circuit.Operators with
                    | Some opA, Some opB ->
                        let aIsMap = opA.GetType().Name.StartsWith("ZSetMapOperator")
                        let bIsFilter = opB.GetType().Name.StartsWith("ZSetFilterOperator")
                        aIsMap && bIsFilter
                        && isSingleProducerTarget circuit.Connections b
                        && isSingleConsumerSource circuit.Connections a
                    | _ -> false)
            member _.Apply(circuit) =
                let tryFuse (a:NodeId,b:NodeId) =
                    match Map.tryFind a circuit.Operators, Map.tryFind b circuit.Operators with
                    | Some opA, Some opB ->
                        let aIsMap = opA.GetType().Name.StartsWith("ZSetMapOperator")
                        let bIsFilter = opB.GetType().Name.StartsWith("ZSetFilterOperator")
                        if aIsMap && bIsFilter then
                            let mapObj = opA.GetType().GetProperty("Transform").GetValue(opA)
                            let predObj = opB.GetType().GetProperty("Predicate").GetValue(opB)
                            let mapType = opA.GetType()
                            let genArgs = mapType.GetGenericArguments()
                            let inTy = genArgs.[0]
                            let outTy = genArgs.[1]
                            let fusedTy = typeof<MapFilterOperator<_,_>>.GetGenericTypeDefinition().MakeGenericType([| inTy; outTy |])
                            let fused = Activator.CreateInstance(fusedTy, [| mapObj; predObj |])
                            let ops' = circuit.Operators |> Map.remove a |> Map.add b fused
                            let conns' =
                                circuit.Connections
                                |> List.collect (fun (s,t) ->
                                    if s = a && t = b then []
                                    elif t = a then [ (s, b) ]
                                    elif s = a then []
                                    else [ (s,t) ])
                            Some {|
                                circuit with
                                    Operators = ops'
                                    Connections = conns'
                            |}
                        else None
                    | _ -> None
                match circuit.Connections |> List.tryPick tryFuse with
                | Some c -> c
                | None -> circuit
    
    /// Build rules with options
    let private rulesWith (options: OptimizerOptions) : IOptimizationRule list =
        let baseRules = standardRules
        if options.EnableFusion then
            baseRules @ [ FilterMapFusionRule() :> IOptimizationRule; MapFilterFusionRule() :> IOptimizationRule ]
        else baseRules
    
    /// Create optimizer with standard rules
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let createStandard() = CircuitOptimizer(standardRules)
    
    /// Create optimizer with options
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let createWith (options: OptimizerOptions) = CircuitOptimizer(rulesWith options)
    
    /// Optimize circuit with standard optimizations
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let optimize (circuit: CircuitDefinition) : CircuitDefinition =
        let optimizer = createWith { EnableFusion = true }
        optimizer.OptimizeOnce(circuit)

    /// Optimize circuit with options
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let optimizeWithOptions (options: OptimizerOptions) (circuit: CircuitDefinition) : CircuitDefinition =
        let optimizer = createWith options
        optimizer.OptimizeOnce(circuit)
