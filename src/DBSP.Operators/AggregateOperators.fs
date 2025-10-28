/// Aggregation operators for DBSP circuits with incremental state management
/// Implements GROUP BY, SUM, COUNT, AVG and other aggregation functions
module DBSP.Operators.AggregateOperators

open System.Threading.Tasks
open FSharp.Data.Adaptive
open DBSP.Core.ZSet
open DBSP.Core.IndexedZSet
open DBSP.Operators.Interfaces

/// Generic aggregation operator that maintains incremental state by key
type AggregateOperator<'K, 'V, 'Acc when 'K: comparison and 'V: comparison and 'Acc: comparison>(
    initialAcc: 'Acc,
    updateFn: 'Acc -> 'V -> int -> 'Acc,
    ?name: string) =
    inherit BaseUnaryOperator<ZSet<'K * 'V>, ZSet<'K * 'Acc>>(defaultArg name "Aggregate")
    
    // Maintained state mapping keys to their accumulated values
    let mutable state: HashMap<'K, 'Acc> = HashMap.empty
    
    override _.EvalAsyncImpl(input: ZSet<'K * 'V>) = task {
        // Process changes incrementally
        let mutable updatedState = state
        
        let backend = System.Environment.GetEnvironmentVariable("DBSP_BACKEND")
        let useArranged = System.String.Equals(backend, "Adaptive", System.StringComparison.OrdinalIgnoreCase) && input.Count > 1000
        if useArranged then
            use _view = ZSet.arrangedView input
            for ((key, value), weight) in ZSet.toSeq input do
                if weight <> 0 then
                    let currentAcc = HashMap.tryFind key updatedState |> Option.defaultValue initialAcc
                    let newAcc = updateFn currentAcc value weight
                    updatedState <- HashMap.add key newAcc updatedState
        else
            ZSet.fold (fun acc (key, value) weight ->
                let currentAcc = HashMap.tryFind key acc |> Option.defaultValue initialAcc
                let newAcc = updateFn currentAcc value weight
                HashMap.add key newAcc acc
            ) updatedState input |> fun newState -> updatedState <- newState
        
        state <- updatedState
        
        // Convert state back to ZSet format for output (single build)
        let result =
            ZSet.buildWith (fun b ->
                HashMap.iter (fun key aggValue -> b.Add((key, aggValue), 1)) updatedState
            )
        
        return result
    }
    
    interface IStatefulOperator<HashMap<'K, 'Acc>> with
        member _.GetState() = state
        member _.SetState(newState) = state <- newState
        member _.SerializeState() = task {
            // Simple string serialization for now
            let stateArray = HashMap.toArray state
            let stateStr = sprintf "%A" stateArray
            return System.Text.Encoding.UTF8.GetBytes(stateStr)
        }
        member _.DeserializeState(data: byte[]) = task {
            // Reset to empty for now - production would use proper deserialization
            state <- HashMap.empty
            return ()
        }

/// Count operator - counts elements by key
type CountOperator<'K, 'V when 'K: comparison and 'V: comparison>(?name: string) =
    inherit BaseUnaryOperator<ZSet<'K * 'V>, ZSet<'K * int64>>(defaultArg name "Count")
    
    let mutable counts: HashMap<'K, int64> = HashMap.empty
    
    override _.EvalAsyncImpl(input: ZSet<'K * 'V>) = task {
        // Update counts based on weight changes
        let mutable updatedCounts = counts
        
        let backend = System.Environment.GetEnvironmentVariable("DBSP_BACKEND")
        let useArranged = System.String.Equals(backend, "Adaptive", System.StringComparison.OrdinalIgnoreCase) && input.Count > 1000
        if useArranged then
            use _view = ZSet.arrangedView input
            for ((key, _value), weight) in ZSet.toSeq input do
                if weight <> 0 then
                    let currentCount = HashMap.tryFind key updatedCounts |> Option.defaultValue 0L
                    let newCount = currentCount + int64 weight
                    updatedCounts <- if newCount = 0L then HashMap.remove key updatedCounts else HashMap.add key newCount updatedCounts
        else
            ZSet.fold (fun acc (key, _value) weight ->
                let currentCount = HashMap.tryFind key acc |> Option.defaultValue 0L
                let newCount = currentCount + int64 weight
                if newCount = 0L then
                    HashMap.remove key acc  // Remove keys with zero count
                else
                    HashMap.add key newCount acc
            ) updatedCounts input |> fun newCounts -> updatedCounts <- newCounts
        
        counts <- updatedCounts
        
        // Convert to ZSet output (single build)
        let result =
            ZSet.buildWith (fun b ->
                HashMap.iter (fun key count -> b.Add((key, count), 1)) updatedCounts
            )
        
        return result
    }

/// Sum operator - sums numeric values by key  
type SumOperator<'K, 'V when 'K: comparison and 'V: comparison>(?name: string) =
    inherit AggregateOperator<'K, 'V, 'V>(
        initialAcc = Unchecked.defaultof<'V>,  // Default zero value
        updateFn = (fun acc value weight -> 
            // Add weight * value to accumulator
            // This is simplified - production would use proper numeric operations
            acc), // Placeholder - needs numeric constraints
        ?name = name)

/// Specific sum operators for common numeric types

/// Integer sum operator
type IntSumOperator<'K when 'K: comparison>(?name: string) =
    inherit AggregateOperator<'K, int, int>(
        0,
        (fun acc value weight -> acc + (value * weight)),
        defaultArg name "IntSum")

/// Floating point sum operator
type FloatSumOperator<'K when 'K: comparison>(?name: string) =
    inherit AggregateOperator<'K, float, float>(
        0.0,
        (fun acc value weight -> acc + (value * float weight)),
        defaultArg name "FloatSum")

/// Average operator - maintains sum and count for incremental average calculation
type AverageOperator<'K when 'K: comparison>(?name: string) =
    inherit BaseUnaryOperator<ZSet<'K * float>, ZSet<'K * float>>(defaultArg name "Average")
    
    // State: (sum, count) for each key
    let mutable state: HashMap<'K, float * int64> = HashMap.empty
    
    override _.EvalAsyncImpl(input: ZSet<'K * float>) = task {
        // Update sums and counts incrementally
        let mutable updatedState = state
        
        ZSet.fold (fun acc (key, value) weight ->
            let (currentSum, currentCount) = HashMap.tryFind key acc |> Option.defaultValue (0.0, 0L)
            let newSum = currentSum + (value * float weight)
            let newCount = currentCount + int64 weight
            
            if newCount = 0L then
                HashMap.remove key acc  // Remove keys with zero count
            else
                HashMap.add key (newSum, newCount) acc
        ) updatedState input |> fun newState -> updatedState <- newState
        
        state <- updatedState
        
        // Calculate averages and convert to ZSet output (single build)
        let result =
            ZSet.buildWith (fun b ->
                HashMap.iter (fun key (sum, count) ->
                    let average = sum / float count
                    b.Add((key, average), 1)
                ) updatedState
            )
        
        return result
    }

/// GroupBy operator - groups Z-set elements by a key function
type GroupByOperator<'T, 'K when 'T: comparison and 'K: comparison>(keyFn: 'T -> 'K, ?name: string) =
    inherit BaseUnaryOperator<ZSet<'T>, IndexedZSet<'K, 'T>>(defaultArg name "GroupBy")
    
    override _.EvalAsyncImpl(input: ZSet<'T>) = task {
        // Use arranged view when Adaptive and input is large to improve scan locality
        let backend = System.Environment.GetEnvironmentVariable("DBSP_BACKEND")
        let useArranged = System.String.Equals(backend, "Adaptive", System.StringComparison.OrdinalIgnoreCase) && input.Count > 1000
        let result =
            if useArranged then
                use _view = ZSet.arrangedView input
                IndexedZSet.groupBy keyFn input
            else
                IndexedZSet.groupBy keyFn input
        return result
    }

/// Distinct operator - removes duplicate entries (normalizes weights to ±1)
type DistinctOperator<'K when 'K: comparison>(?name: string) =
    inherit BaseUnaryOperator<ZSet<'K>, ZSet<'K>>(defaultArg name "Distinct")
    
    override _.EvalAsyncImpl(input: ZSet<'K>) = task {
        // Normalize all weights to ±1 based on sign
        let result =
            ZSet.buildWith (fun b ->
                ZSet.iter (fun key weight ->
                    if weight > 0 then b.Add(key, 1)
                    elif weight < 0 then b.Add(key, -1)
                    else ()
                ) input
            )
        return result
    }

/// Module functions for creating aggregation operators
module AggregateOperators =

    /// Create a generic aggregation operator
    let aggregate initialAcc updateFn : IUnaryOperator<ZSet<'K * 'V>, ZSet<'K * 'Acc>> =
        AggregateOperator(initialAcc, updateFn) :> IUnaryOperator<ZSet<'K * 'V>, ZSet<'K * 'Acc>>

    /// Create a count operator
    let count<'K, 'V when 'K: comparison and 'V: comparison> : IUnaryOperator<ZSet<'K * 'V>, ZSet<'K * int64>> =
        CountOperator() :> IUnaryOperator<ZSet<'K * 'V>, ZSet<'K * int64>>

    /// Create an integer sum operator
    let intSum<'K when 'K: comparison> : IUnaryOperator<ZSet<'K * int>, ZSet<'K * int>> =
        IntSumOperator() :> IUnaryOperator<ZSet<'K * int>, ZSet<'K * int>>

    /// Create a floating point sum operator
    let floatSum<'K when 'K: comparison> : IUnaryOperator<ZSet<'K * float>, ZSet<'K * float>> =
        FloatSumOperator() :> IUnaryOperator<ZSet<'K * float>, ZSet<'K * float>>

    /// Create an average operator
    let average<'K when 'K: comparison> : IUnaryOperator<ZSet<'K * float>, ZSet<'K * float>> =
        AverageOperator() :> IUnaryOperator<ZSet<'K * float>, ZSet<'K * float>>

    /// Create a group-by operator
    let groupBy keyFn : IUnaryOperator<ZSet<'T>, IndexedZSet<'K, 'T>> =
        GroupByOperator(keyFn) :> IUnaryOperator<ZSet<'T>, IndexedZSet<'K, 'T>>

    /// Create a distinct operator
    let distinct<'K when 'K: comparison> : IUnaryOperator<ZSet<'K>, ZSet<'K>> =
        DistinctOperator() :> IUnaryOperator<ZSet<'K>, ZSet<'K>>
