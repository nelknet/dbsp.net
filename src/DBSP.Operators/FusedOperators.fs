/// Fused operators that combine multiple operations to eliminate intermediate materializations
module DBSP.Operators.FusedOperators

open System.Threading.Tasks
open DBSP.Core
open DBSP.Core.ZSet
// using ZSet.buildWith for efficient construction
open DBSP.Operators.Interfaces
open FSharp.Data.Adaptive

/// Fused Map-Filter operator that performs both operations in a single pass
type MapFilterOperator<'In, 'Out when 'In: comparison and 'Out: comparison>
    (mapFn: 'In -> 'Out, filterPredicate: 'Out -> bool) =
    
    interface IUnaryOperator<ZSet<'In>, ZSet<'Out>> with
        member _.Name = "MapFilter"
        member _.IsAsync = false
        member _.Ready = true
        member _.Fixedpoint(_) = true
        member _.Flush() = ()
        member _.ClearState() = ()
        member _.InputPreference = OwnershipPreference.PreferRef
        member _.EvalAsync(input: ZSet<'In>) =
            let result =
                ZSet.buildWith (fun builder ->
                    for (item, weight) in HashMap.toSeq input.Inner do
                        if weight <> 0 then
                            let mapped = mapFn item
                            if filterPredicate mapped then
                                builder.Add(mapped, weight)
                )
            Task.FromResult(result)

/// Fused Filter-Map operator (filter first for efficiency)
type FilterMapOperator<'In, 'Out when 'In: comparison and 'Out: comparison>
    (filterPredicate: 'In -> bool, mapFn: 'In -> 'Out) =
    
    interface IUnaryOperator<ZSet<'In>, ZSet<'Out>> with
        member _.Name = "FilterMap"
        member _.IsAsync = false
        member _.Ready = true
        member _.Fixedpoint(_) = true
        member _.Flush() = ()
        member _.ClearState() = ()
        member _.InputPreference = OwnershipPreference.PreferRef
        member _.EvalAsync(input: ZSet<'In>) =
            let result =
                ZSet.buildWith (fun builder ->
                    for (item, weight) in HashMap.toSeq input.Inner do
                        if weight <> 0 && filterPredicate item then
                            builder.Add(mapFn item, weight)
                )
            Task.FromResult(result)

/// Fused Map-GroupBy operator for efficient aggregation pipelines
type MapGroupByOperator<'In, 'Mid, 'Key, 'Out when 'In: comparison and 'Mid: comparison 
                                                 and 'Key: comparison and 'Out: comparison>
    (mapFn: 'In -> 'Mid, keyFn: 'Mid -> 'Key, aggregateFn: seq<'Mid * int> -> 'Out) =
    
    interface IUnaryOperator<ZSet<'In>, ZSet<'Key * 'Out>> with
        member _.Name = "MapGroupBy"
        member _.IsAsync = false
        member _.Ready = true
        member _.Fixedpoint(_) = true
        member _.Flush() = ()
        member _.ClearState() = ()
        member _.InputPreference = OwnershipPreference.PreferRef
        member _.EvalAsync(input: ZSet<'In>) =
            // Single pass: map and group simultaneously
            let groups = System.Collections.Generic.Dictionary<'Key, ResizeArray<'Mid * int>>()
            
            for (item, weight) in HashMap.toSeq input.Inner do
                if weight <> 0 then
                    let mapped = mapFn item
                    let key = keyFn mapped
                    
                    match groups.TryGetValue(key) with
                    | true, group -> group.Add((mapped, weight))
                    | false, _ -> 
                        let group = ResizeArray<_>()
                        group.Add((mapped, weight))
                        groups.[key] <- group
            
            // Build result
            let result =
                ZSet.buildWith (fun builder ->
                    for kv in groups do
                        let key = kv.Key
                        let items = kv.Value
                        let aggregated = aggregateFn items
                        builder.Add((key, aggregated), 1)
                )
            Task.FromResult(result)

/// Fused Filter-GroupBy-Aggregate operator
type FilterGroupByAggregateOperator<'In, 'Key, 'Acc when 'In: comparison and 'Key: comparison and 'Acc: comparison>
    (filterPredicate: 'In -> bool,
     keyFn: 'In -> 'Key,
     seed: 'Acc,
     folder: 'Acc -> 'In -> int -> 'Acc) =
    
    interface IUnaryOperator<ZSet<'In>, ZSet<'Key * 'Acc>> with
        member _.Name = "FilterGroupByAggregate"
        member _.IsAsync = false
        member _.Ready = true
        member _.Fixedpoint(_) = true
        member _.Flush() = ()
        member _.ClearState() = ()
        member _.InputPreference = OwnershipPreference.PreferRef
        member _.EvalAsync(input: ZSet<'In>) =
            let groups = System.Collections.Generic.Dictionary<'Key, 'Acc>()
            
            // Single pass: filter, group, and aggregate
            for (item, weight) in HashMap.toSeq input.Inner do
                if weight <> 0 && filterPredicate item then
                    let key = keyFn item
                    
                    let acc = 
                        match groups.TryGetValue(key) with
                        | true, existing -> existing
                        | false, _ -> seed
                    
                    groups.[key] <- folder acc item weight
            
            // Build result
            let result =
                ZSet.buildWith (fun builder ->
                    for kv in groups do
                        let key = kv.Key
                        let acc = kv.Value
                        builder.Add((key, acc), 1)
                )
            Task.FromResult(result)

/// Fused Join-Map operator for efficient join pipelines
type JoinMapOperator<'K, 'V1, 'V2, 'Out when 'K: comparison and 'V1: comparison 
                                          and 'V2: comparison and 'Out: comparison>
    (joinKeyLeft: 'V1 -> 'K,
     joinKeyRight: 'V2 -> 'K,
     mapResult: 'V1 -> 'V2 -> 'Out) =
    
    let mutable leftState = FSharp.Data.Adaptive.HashMap.empty<'K, ResizeArray<'V1 * int>>
    let mutable rightState = FSharp.Data.Adaptive.HashMap.empty<'K, ResizeArray<'V2 * int>>
    
    interface IBinaryOperator<ZSet<'V1>, ZSet<'V2>, ZSet<'Out>> with
        member _.Name = "JoinMap"
        member _.IsAsync = false
        member _.Ready = true
        member _.Fixedpoint(_) = true
        member _.Flush() = ()
        member _.ClearState() = ()
        member _.InputPreferences = (OwnershipPreference.PreferRef, OwnershipPreference.PreferRef)
        member _.EvalAsync(leftDelta: ZSet<'V1>) (rightDelta: ZSet<'V2>) =
            // Update left state
            for (item, weight) in HashMap.toSeq leftDelta.Inner do
                if weight <> 0 then
                    let key = joinKeyLeft item
                    let items = 
                        match FSharp.Data.Adaptive.HashMap.tryFind key leftState with
                        | Some list -> list
                        | None -> 
                            let list = ResizeArray<_>()
                            leftState <- FSharp.Data.Adaptive.HashMap.add key list leftState
                            list
                    items.Add((item, weight))
            
            // Update right state
            for (item, weight) in HashMap.toSeq rightDelta.Inner do
                if weight <> 0 then
                    let key = joinKeyRight item
                    let items = 
                        match FSharp.Data.Adaptive.HashMap.tryFind key rightState with
                        | Some list -> list
                        | None -> 
                            let list = ResizeArray<_>()
                            rightState <- FSharp.Data.Adaptive.HashMap.add key list rightState
                            list
                    items.Add((item, weight))
            
            // Compute join with integrated mapping
            let result = ZSet.buildWith (fun builder ->
                // Process new left items against all right
                for (leftItem, leftWeight) in HashMap.toSeq leftDelta.Inner do
                    if leftWeight <> 0 then
                        let key = joinKeyLeft leftItem
                        match FSharp.Data.Adaptive.HashMap.tryFind key rightState with
                        | Some rightItems ->
                            for (rightItem, rightWeight) in rightItems do
                                if rightWeight <> 0 then
                                    let result = mapResult leftItem rightItem
                                    builder.Add(result, leftWeight * rightWeight)
                        | None -> ()
                
                // Process new right items against existing left (avoiding duplicates)
                for (rightItem, rightWeight) in HashMap.toSeq rightDelta.Inner do
                    if rightWeight <> 0 then
                        let key = joinKeyRight rightItem
                        match FSharp.Data.Adaptive.HashMap.tryFind key leftState with
                        | Some leftItems ->
                            for (leftItem, leftWeight) in leftItems do
                                // Skip items from current delta (already processed)
                                if not (leftDelta.Inner.ContainsKey(leftItem)) && leftWeight <> 0 then
                                    let result = mapResult leftItem rightItem
                                    builder.Add(result, leftWeight * rightWeight)
                        | None -> ()
            )
            Task.FromResult(result)

/// Named fused Join-Project operator (thin wrapper over JoinMap)
type JoinProjectOperator<'K, 'V1, 'V2, 'Out when 'K: comparison and 'V1: comparison 
                                            and 'V2: comparison and 'Out: comparison>
    (joinKeyLeft: 'V1 -> 'K,
     joinKeyRight: 'V2 -> 'K,
     project: 'V1 -> 'V2 -> 'Out) =
    
    let inner = JoinMapOperator<'K,'V1,'V2,'Out>(joinKeyLeft, joinKeyRight, project)
    
    interface IBinaryOperator<ZSet<'V1>, ZSet<'V2>, ZSet<'Out>> with
        member _.Name = "JoinProject"
        member _.IsAsync = false
        member _.Ready = true
        member _.Fixedpoint(_) = true
        member _.Flush() = ()
        member _.ClearState() = ()
        member _.InputPreferences = (OwnershipPreference.PreferRef, OwnershipPreference.PreferRef)
        member _.EvalAsync(leftDelta: ZSet<'V1>) (rightDelta: ZSet<'V2>) =
            (inner :> IBinaryOperator<ZSet<'V1>, ZSet<'V2>, ZSet<'Out>>).EvalAsync(leftDelta)(rightDelta)

/// Fused pipeline operator that chains multiple operations
type PipelineOperator<'In, 'Out when 'In: comparison and 'Out: comparison>
    (operations: ('In -> 'Out option) list) =
    
    interface IUnaryOperator<ZSet<'In>, ZSet<'Out>> with
        member _.Name = "Pipeline"
        member _.IsAsync = false
        member _.Ready = true
        member _.Fixedpoint(_) = true
        member _.Flush() = ()
        member _.ClearState() = ()
        member _.InputPreference = OwnershipPreference.PreferRef
        member _.EvalAsync(input: ZSet<'In>) =
            let result =
                ZSet.buildWith (fun builder ->
                    for (item, weight) in HashMap.toSeq input.Inner do
                        if weight <> 0 then
                            let rec applyOps (value: obj) (ops: ('In -> 'Out option) list) =
                                match ops with
                                | [] -> 
                                    match value with
                                    | :? 'Out as result -> Some result
                                    | _ -> None
                                | op :: rest ->
                                    match value with
                                    | :? 'In as input ->
                                        match op input with
                                        | Some output -> applyOps (box output) rest
                                        | None -> None
                                    | _ -> None
                            
                            match applyOps (box item) operations with
                            | Some result -> builder.Add(result, weight)
                            | None -> ()
                )
            Task.FromResult(result)

/// Module for creating and composing fused operators
module FusedOperators =
    
    /// Create a fused map-filter operator
    let mapFilter (mapFn: 'In -> 'Out) (filterPredicate: 'Out -> bool) =
        MapFilterOperator(mapFn, filterPredicate) :> IUnaryOperator<ZSet<'In>, ZSet<'Out>>
    
    /// Create a fused filter-map operator
    let filterMap (filterPredicate: 'In -> bool) (mapFn: 'In -> 'Out) =
        FilterMapOperator(filterPredicate, mapFn) :> IUnaryOperator<ZSet<'In>, ZSet<'Out>>
    
    /// Create a fused map-groupBy operator
    let mapGroupBy (mapFn: 'In -> 'Mid) (keyFn: 'Mid -> 'Key) (aggregateFn: seq<'Mid * int> -> 'Out) =
        MapGroupByOperator(mapFn, keyFn, aggregateFn) :> IUnaryOperator<ZSet<'In>, ZSet<'Key * 'Out>>
    
    /// Create a fused filter-groupBy-aggregate operator
    let filterGroupByAggregate (filterPredicate: 'In -> bool) (keyFn: 'In -> 'Key) (seed: 'Acc) (folder: 'Acc -> 'In -> int -> 'Acc) =
        FilterGroupByAggregateOperator(filterPredicate, keyFn, seed, folder) :> IUnaryOperator<ZSet<'In>, ZSet<'Key * 'Acc>>
    
    /// Create a fused join-map operator
    let joinMap (joinKeyLeft: 'V1 -> 'K) (joinKeyRight: 'V2 -> 'K) (mapResult: 'V1 -> 'V2 -> 'Out) =
        JoinMapOperator(joinKeyLeft, joinKeyRight, mapResult) :> IBinaryOperator<ZSet<'V1>, ZSet<'V2>, ZSet<'Out>>
    
    /// Compose multiple unary operators into a single fused operator
    let compose (operators: IUnaryOperator<'a, 'b> list) =
        { new IUnaryOperator<'a, 'b> with
            member _.Name = "Composed"
            member _.IsAsync = false
            member _.Ready = true
            member _.Fixedpoint(_) = true
            member _.Flush() = ()
            member _.ClearState() = ()
            member _.InputPreference = OwnershipPreference.PreferRef
            member _.EvalAsync(input) = task {
                let mutable result = box input
                for op in operators do
                    let! nextResult = 
                        match result with
                        | :? 'a as typedInput -> op.EvalAsync(typedInput)
                        | _ -> failwith "Type mismatch in operator composition"
                    result <- box nextResult
                return unbox result
            }
        }
