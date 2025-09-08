/// Linear operators for DBSP circuits: Map, Filter, FlatMap
/// These are stateless operators that transform inputs to outputs
module DBSP.Operators.LinearOperators

open System.Runtime.CompilerServices
open System.Threading.Tasks
open DBSP.Core.ZSet
open DBSP.Operators.Interfaces

/// Map operator - applies a transformation function to each element
type MapOperator<'I, 'O>([<InlineIfLambda>] transform: 'I -> 'O, ?name: string) =
    inherit BaseUnaryOperator<'I, 'O>(defaultArg name "Map")
    
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    override _.EvalAsyncImpl(input: 'I) = task {
        // Synchronous transformation wrapped in Task for consistency
        return transform input
    }
    
    interface IUnaryOperator<'I, 'O> with
        member _.InputPreference = PreferRef  // Prefer reference to avoid copying

/// Map operator specifically for Z-sets - transforms keys while preserving weights
type ZSetMapOperator<'K1, 'K2 when 'K1: comparison and 'K2: comparison>([<InlineIfLambda>] transform: 'K1 -> 'K2, ?name: string) =
    inherit BaseUnaryOperator<ZSet<'K1>, ZSet<'K2>>(defaultArg name "ZSetMap")
    
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    override _.EvalAsyncImpl(input: ZSet<'K1>) = task {
        // Transform all keys in the ZSet while preserving weights
        let result = ZSet.mapKeys transform input
        return result
    }

/// Filter operator - removes elements that don't satisfy a predicate
type FilterOperator<'T>([<InlineIfLambda>] predicate: 'T -> bool, ?name: string) =
    inherit BaseUnaryOperator<'T, 'T option>(defaultArg name "Filter")
    
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    override _.EvalAsyncImpl(input: 'T) = task {
        if predicate input then
            return Some input
        else
            return None
    }

/// Filter operator specifically for Z-sets - filters keys while preserving structure
type ZSetFilterOperator<'K when 'K: comparison>([<InlineIfLambda>] predicate: 'K -> bool, ?name: string) =
    inherit BaseUnaryOperator<ZSet<'K>, ZSet<'K>>(defaultArg name "ZSetFilter")
    
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    override _.EvalAsyncImpl(input: ZSet<'K>) = task {
        // Filter keys that satisfy the predicate
        let result = ZSet.filter predicate input
        return result
    }

/// FlatMap operator - applies a function that produces sequences and flattens the result
type FlatMapOperator<'I, 'O>([<InlineIfLambda>] transform: 'I -> seq<'O>, ?name: string) =
    inherit BaseUnaryOperator<'I, seq<'O>>(defaultArg name "FlatMap")
    
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    override _.EvalAsyncImpl(input: 'I) = task {
        return transform input
    }

/// FlatMap operator for Z-sets - transforms each key to multiple keys while distributing weights
type ZSetFlatMapOperator<'K1, 'K2 when 'K1: comparison and 'K2: comparison>(
    [<InlineIfLambda>] transform: 'K1 -> seq<'K2>, ?name: string) =
    inherit BaseUnaryOperator<ZSet<'K1>, ZSet<'K2>>(defaultArg name "ZSetFlatMap")
    
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    override _.EvalAsyncImpl(input: ZSet<'K1>) = task {
        // Transform each key to multiple keys, distributing the weight
        let result = 
            ZSet.fold (fun acc key weight ->
                let newKeys = transform key
                newKeys |> Seq.fold (fun acc2 newKey ->
                    ZSet.insert newKey weight acc2
                ) acc
            ) (ZSet.empty<'K2>) input
        return result
    }

/// Negation operator - negates all weights in a Z-set
type NegateOperator<'K when 'K: comparison>(?name: string) =
    inherit BaseUnaryOperator<ZSet<'K>, ZSet<'K>>(defaultArg name "Negate")
    
    override _.EvalAsyncImpl(input: ZSet<'K>) = task {
        return ZSet.negate input
    }

/// Union operator - combines two Z-sets by adding weights  
type UnionOperator<'K when 'K: comparison>(?name: string) =
    inherit BaseBinaryOperator<ZSet<'K>, ZSet<'K>, ZSet<'K>>(defaultArg name "Union")
    
    override _.EvalAsyncImpl left right = task {
        return ZSet.add left right
    }

/// Minus operator - subtracts second Z-set from first by negating and adding
type MinusOperator<'K when 'K: comparison>(?name: string) =
    inherit BaseBinaryOperator<ZSet<'K>, ZSet<'K>, ZSet<'K>>(defaultArg name "Minus")
    
    override _.EvalAsyncImpl left right = task {
        return ZSet.difference left right
    }

/// Distinct operator - removes duplicates by setting all weights to 1 (if positive) or -1 (if negative)
type DistinctOperator<'K when 'K: comparison>(?name: string) =
    inherit BaseUnaryOperator<ZSet<'K>, ZSet<'K>>(defaultArg name "Distinct")
    
    override _.EvalAsyncImpl(input: ZSet<'K>) = task {
        // Convert to distinct by normalizing weights to ±1
        let distinctResult = 
            ZSet.fold (fun acc key weight ->
                if weight > 0 then
                    ZSet.insert key 1 acc
                elif weight < 0 then
                    ZSet.insert key (-1) acc
                else
                    acc // Skip zero weights
            ) (ZSet.empty<'K>) input
        return distinctResult
    }

/// Module functions for creating operators
module LinearOperators =

    /// Create a map operator
    let map (transform: 'I -> 'O) : IUnaryOperator<'I, 'O> =
        MapOperator(transform) :> IUnaryOperator<'I, 'O>

    /// Create a filter operator
    let filter (predicate: 'T -> bool) : IUnaryOperator<'T, 'T option> =
        FilterOperator(predicate) :> IUnaryOperator<'T, 'T option>

    /// Create a FlatMap operator
    let flatMap (transform: 'I -> seq<'O>) : IUnaryOperator<'I, seq<'O>> =
        FlatMapOperator(transform) :> IUnaryOperator<'I, seq<'O>>

    /// Create a Z-set specific map operator
    let zsetMap (transform: 'K1 -> 'K2) : IUnaryOperator<ZSet<'K1>, ZSet<'K2>> =
        ZSetMapOperator(transform) :> IUnaryOperator<ZSet<'K1>, ZSet<'K2>>

    /// Create a Z-set specific filter operator
    let zsetFilter (predicate: 'K -> bool) : IUnaryOperator<ZSet<'K>, ZSet<'K>> =
        ZSetFilterOperator(predicate) :> IUnaryOperator<ZSet<'K>, ZSet<'K>>

    /// Create a Z-set specific FlatMap operator
    let zsetFlatMap (transform: 'K1 -> seq<'K2>) : IUnaryOperator<ZSet<'K1>, ZSet<'K2>> =
        ZSetFlatMapOperator(transform) :> IUnaryOperator<ZSet<'K1>, ZSet<'K2>>

    /// Create a negation operator for Z-sets
    let negate<'K when 'K: comparison> : IUnaryOperator<ZSet<'K>, ZSet<'K>> =
        NegateOperator() :> IUnaryOperator<ZSet<'K>, ZSet<'K>>

    /// Create a union operator for Z-sets
    let union<'K when 'K: comparison> : IBinaryOperator<ZSet<'K>, ZSet<'K>, ZSet<'K>> =
        UnionOperator() :> IBinaryOperator<ZSet<'K>, ZSet<'K>, ZSet<'K>>

    /// Create a minus operator for Z-sets
    let minus<'K when 'K: comparison> : IBinaryOperator<ZSet<'K>, ZSet<'K>, ZSet<'K>> =
        MinusOperator() :> IBinaryOperator<ZSet<'K>, ZSet<'K>, ZSet<'K>>

    /// Create a distinct operator for Z-sets
    let distinct<'K when 'K: comparison> : IUnaryOperator<ZSet<'K>, ZSet<'K>> =
        DistinctOperator() :> IUnaryOperator<ZSet<'K>, ZSet<'K>>