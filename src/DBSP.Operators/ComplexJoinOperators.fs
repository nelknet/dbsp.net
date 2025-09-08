/// Complex join operators: outer joins, semi-joins, anti-joins
/// Extends basic join functionality with advanced join semantics
module DBSP.Operators.ComplexJoinOperators

open System.Runtime.CompilerServices
open System.Threading.Tasks
open DBSP.Core.ZSet
open DBSP.Core.IndexedZSet
open DBSP.Operators.Interfaces
open FSharp.Data.Adaptive

/// Left outer join operator - includes all left records, with nulls for unmatched right
type LeftOuterJoinOperator<'K, 'V1, 'V2 when 'K: comparison and 'V1: comparison and 'V2: comparison>(?name: string) =
    inherit BaseBinaryOperator<ZSet<'K * 'V1>, ZSet<'K * 'V2>, ZSet<'K * 'V1 * 'V2 option>>(
        defaultArg name "LeftOuterJoin")
    
    let mutable leftState: IndexedZSet<'K, 'V1> = IndexedZSet.empty
    let mutable rightState: IndexedZSet<'K, 'V2> = IndexedZSet.empty
    
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    override _.EvalAsyncImpl leftDelta rightDelta = task {
        let leftIndexed = IndexedZSet.fromZSet leftDelta
        let rightIndexed = IndexedZSet.fromZSet rightDelta
        
        // Update state first to get the new complete state
        let newLeftState = IndexedZSet.add leftState leftIndexed
        let newRightState = IndexedZSet.add rightState rightIndexed
        
        // For left outer join: δL ⟕ (R ∪ δR) + (L \ δL) ⟕ δR
        // This avoids double-counting
        
        // δL ⟕ newRightState (new left with all right)
        let deltaL_allR = 
            leftIndexed.Index
            |> HashMap.toSeq
            |> Seq.collect (fun (k, leftZSet) ->
                match HashMap.tryFind k newRightState.Index with
                | Some rightZSet ->
                    // Inner join for this key
                    ZSet.toSeq leftZSet
                    |> Seq.collect (fun (v1, w1) ->
                        ZSet.toSeq rightZSet
                        |> Seq.map (fun (v2, w2) -> ((k, v1, Some v2), w1 * w2)))
                | None ->
                    // Left-only records
                    ZSet.toSeq leftZSet
                    |> Seq.map (fun (v1, w1) -> ((k, v1, None), w1)))
            |> ZSet.ofSeq
        
        // oldL ⟕ δR (existing left with new right only)
        let oldL_deltaR = 
            rightIndexed.Index
            |> HashMap.toSeq
            |> Seq.collect (fun (k, rightZSet) ->
                match HashMap.tryFind k leftState.Index with
                | Some leftZSet ->
                    // Inner join for this key (only old left)
                    ZSet.toSeq leftZSet
                    |> Seq.collect (fun (v1, w1) ->
                        ZSet.toSeq rightZSet
                        |> Seq.map (fun (v2, w2) -> ((k, v1, Some v2), w1 * w2)))
                | None ->
                    // No old left records for this key
                    Seq.empty)
            |> ZSet.ofSeq
        
        // Update state
        leftState <- newLeftState
        rightState <- newRightState
        
        // Combine results
        return ZSet.union deltaL_allR oldL_deltaR
    }
    
    /// Reset operator state
    member _.Reset() =
        leftState <- IndexedZSet.empty
        rightState <- IndexedZSet.empty
    
    interface IBinaryOperator<ZSet<'K * 'V1>, ZSet<'K * 'V2>, ZSet<'K * 'V1 * 'V2 option>> with
        member this.EvalAsync left right = this.EvalAsyncImpl left right
        member _.InputPreferences = (NoPreference, NoPreference)

/// Right outer join operator - includes all right records, with nulls for unmatched left
type RightOuterJoinOperator<'K, 'V1, 'V2 when 'K: comparison and 'V1: comparison and 'V2: comparison>(?name: string) =
    inherit BaseBinaryOperator<ZSet<'K * 'V1>, ZSet<'K * 'V2>, ZSet<'K * 'V1 option * 'V2>>(
        defaultArg name "RightOuterJoin")
    
    let mutable leftState: IndexedZSet<'K, 'V1> = IndexedZSet.empty
    let mutable rightState: IndexedZSet<'K, 'V2> = IndexedZSet.empty
    
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    override _.EvalAsyncImpl leftDelta rightDelta = task {
        let leftIndexed = IndexedZSet.fromZSet leftDelta
        let rightIndexed = IndexedZSet.fromZSet rightDelta
        
        // Update state first
        let newLeftState = IndexedZSet.add leftState leftIndexed
        let newRightState = IndexedZSet.add rightState rightIndexed
        
        // For right outer join: (L ∪ δL) ⋈ δR + δL ⋈ (R \ δR)
        
        // All left ⋈ δR
        let allL_deltaR = 
            rightIndexed.Index
            |> HashMap.toSeq
            |> Seq.collect (fun (k, rightZSet) ->
                match HashMap.tryFind k newLeftState.Index with
                | Some leftZSet ->
                    // Inner join
                    ZSet.toSeq leftZSet
                    |> Seq.collect (fun (v1, w1) ->
                        ZSet.toSeq rightZSet
                        |> Seq.map (fun (v2, w2) -> ((k, Some v1, v2), w1 * w2)))
                | None ->
                    // Right-only records
                    ZSet.toSeq rightZSet
                    |> Seq.map (fun (v2, w2) -> ((k, None, v2), w2)))
            |> ZSet.ofSeq
        
        // δL ⋈ old R
        let deltaL_oldR = 
            leftIndexed.Index
            |> HashMap.toSeq
            |> Seq.collect (fun (k, leftZSet) ->
                match HashMap.tryFind k rightState.Index with
                | Some rightZSet ->
                    ZSet.toSeq leftZSet
                    |> Seq.collect (fun (v1, w1) ->
                        ZSet.toSeq rightZSet
                        |> Seq.map (fun (v2, w2) -> ((k, Some v1, v2), w1 * w2)))
                | None ->
                    Seq.empty)
            |> ZSet.ofSeq
        
        // Update state
        leftState <- newLeftState
        rightState <- newRightState
        
        return ZSet.union allL_deltaR deltaL_oldR
    }
    
    member _.Reset() =
        leftState <- IndexedZSet.empty
        rightState <- IndexedZSet.empty
    
    interface IBinaryOperator<ZSet<'K * 'V1>, ZSet<'K * 'V2>, ZSet<'K * 'V1 option * 'V2>> with
        member this.EvalAsync left right = this.EvalAsyncImpl left right
        member _.InputPreferences = (NoPreference, NoPreference)

/// Full outer join operator - includes all records from both sides
type FullOuterJoinOperator<'K, 'V1, 'V2 when 'K: comparison and 'V1: comparison and 'V2: comparison>(?name: string) =
    inherit BaseBinaryOperator<ZSet<'K * 'V1>, ZSet<'K * 'V2>, ZSet<'K * 'V1 option * 'V2 option>>(
        defaultArg name "FullOuterJoin")
    
    let mutable leftState: IndexedZSet<'K, 'V1> = IndexedZSet.empty
    let mutable rightState: IndexedZSet<'K, 'V2> = IndexedZSet.empty
    
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    override _.EvalAsyncImpl leftDelta rightDelta = task {
        let leftIndexed = IndexedZSet.fromZSet leftDelta
        let rightIndexed = IndexedZSet.fromZSet rightDelta
        
        // Update state first
        let newLeftState = IndexedZSet.add leftState leftIndexed
        let newRightState = IndexedZSet.add rightState rightIndexed
        
        // For full outer: δL ⟕ all R + old L ⟕ δR
        
        // δL with all right (including left-only)
        let deltaL_allR = 
            leftIndexed.Index
            |> HashMap.toSeq
            |> Seq.collect (fun (k, leftZSet) ->
                match HashMap.tryFind k newRightState.Index with
                | Some rightZSet ->
                    ZSet.toSeq leftZSet
                    |> Seq.collect (fun (v1, w1) ->
                        ZSet.toSeq rightZSet
                        |> Seq.map (fun (v2, w2) -> ((k, Some v1, Some v2), w1 * w2)))
                | None ->
                    // Left-only
                    ZSet.toSeq leftZSet
                    |> Seq.map (fun (v1, w1) -> ((k, Some v1, None), w1)))
            |> ZSet.ofSeq
        
        // old L with δR (including right-only when no old left)
        let oldL_deltaR = 
            rightIndexed.Index
            |> HashMap.toSeq
            |> Seq.collect (fun (k, rightZSet) ->
                match HashMap.tryFind k leftState.Index with
                | Some leftZSet ->
                    ZSet.toSeq leftZSet
                    |> Seq.collect (fun (v1, w1) ->
                        ZSet.toSeq rightZSet
                        |> Seq.map (fun (v2, w2) -> ((k, Some v1, Some v2), w1 * w2)))
                | None ->
                    // Right-only (only if not in new left)
                    if not (HashMap.containsKey k leftIndexed.Index) then
                        ZSet.toSeq rightZSet
                        |> Seq.map (fun (v2, w2) -> ((k, None, Some v2), w2))
                    else
                        Seq.empty)
            |> ZSet.ofSeq
        
        // Update state
        leftState <- newLeftState
        rightState <- newRightState
        
        return ZSet.union deltaL_allR oldL_deltaR
    }
    
    member _.Reset() =
        leftState <- IndexedZSet.empty
        rightState <- IndexedZSet.empty
    
    interface IBinaryOperator<ZSet<'K * 'V1>, ZSet<'K * 'V2>, ZSet<'K * 'V1 option * 'V2 option>> with
        member this.EvalAsync left right = this.EvalAsyncImpl left right
        member _.InputPreferences = (NoPreference, NoPreference)

/// Semi-join operator - returns left records that have matches in right
type SemiJoinOperator<'K, 'V1, 'V2 when 'K: comparison and 'V1: comparison and 'V2: comparison>(?name: string) =
    inherit BaseBinaryOperator<ZSet<'K * 'V1>, ZSet<'K * 'V2>, ZSet<'K * 'V1>>(
        defaultArg name "SemiJoin")
    
    let mutable leftState: IndexedZSet<'K, 'V1> = IndexedZSet.empty
    let mutable rightKeys: Set<'K> = Set.empty
    
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    override _.EvalAsyncImpl leftDelta rightDelta = task {
        let leftIndexed = IndexedZSet.fromZSet leftDelta
        let rightIndexed = IndexedZSet.fromZSet rightDelta
        
        // Track which keys exist in right
        let newRightKeys = 
            rightIndexed.Index
            |> HashMap.toSeq
            |> Seq.map fst
            |> Set.ofSeq
        
        // Semi-join: return left tuples where key exists in right
        let semiJoinResult = 
            leftIndexed.Index
            |> HashMap.toSeq
            |> Seq.collect (fun (k, leftZSet) ->
                if Set.contains k rightKeys || Set.contains k newRightKeys then
                    ZSet.toSeq leftZSet
                    |> Seq.map (fun (v1, weight) -> ((k, v1), weight))
                else
                    Seq.empty)
            |> ZSet.ofSeq
        
        // Also check existing left state against new right keys
        let existingLeftResult = 
            newRightKeys
            |> Set.toSeq
            |> Seq.collect (fun k ->
                match HashMap.tryFind k leftState.Index with
                | Some leftZSet ->
                    ZSet.toSeq leftZSet
                    |> Seq.map (fun (v1, weight) -> ((k, v1), weight))
                | None -> Seq.empty)
            |> ZSet.ofSeq
        
        // Update state
        leftState <- IndexedZSet.add leftState leftIndexed
        rightKeys <- Set.union rightKeys newRightKeys
        
        return ZSet.union semiJoinResult existingLeftResult
    }
    
    member _.Reset() =
        leftState <- IndexedZSet.empty
        rightKeys <- Set.empty
    
    interface IBinaryOperator<ZSet<'K * 'V1>, ZSet<'K * 'V2>, ZSet<'K * 'V1>> with
        member this.EvalAsync left right = this.EvalAsyncImpl left right
        member _.InputPreferences = (NoPreference, NoPreference)

/// Anti-join operator - returns left records that have NO matches in right
type AntiJoinOperator<'K, 'V1, 'V2 when 'K: comparison and 'V1: comparison and 'V2: comparison>(?name: string) =
    inherit BaseBinaryOperator<ZSet<'K * 'V1>, ZSet<'K * 'V2>, ZSet<'K * 'V1>>(
        defaultArg name "AntiJoin")
    
    let mutable leftState: IndexedZSet<'K, 'V1> = IndexedZSet.empty
    let mutable rightKeys: Set<'K> = Set.empty
    
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    override _.EvalAsyncImpl leftDelta rightDelta = task {
        let leftIndexed = IndexedZSet.fromZSet leftDelta
        let rightIndexed = IndexedZSet.fromZSet rightDelta
        
        // Track which keys exist in right
        let newRightKeys = 
            rightIndexed.Index
            |> HashMap.toSeq
            |> Seq.map fst
            |> Set.ofSeq
        
        // Anti-join: return left tuples where key does NOT exist in right
        let antiJoinResult = 
            leftIndexed.Index
            |> HashMap.toSeq
            |> Seq.collect (fun (k, leftZSet) ->
                if not (Set.contains k rightKeys || Set.contains k newRightKeys) then
                    ZSet.toSeq leftZSet
                    |> Seq.map (fun (v1, weight) -> ((k, v1), weight))
                else
                    Seq.empty)
            |> ZSet.ofSeq
        
        // Need to produce negative weights for existing left records that now have matches
        let negativeResults = 
            newRightKeys
            |> Set.toSeq
            |> Seq.collect (fun k ->
                if not (Set.contains k rightKeys) then
                    // This key is newly added to right
                    match HashMap.tryFind k leftState.Index with
                    | Some leftZSet ->
                        // These left records should no longer be in the result
                        ZSet.toSeq leftZSet
                        |> Seq.map (fun (v1, weight) -> ((k, v1), -weight))
                    | None -> Seq.empty
                else
                    Seq.empty)
            |> ZSet.ofSeq
        
        // Update state
        leftState <- IndexedZSet.add leftState leftIndexed
        rightKeys <- Set.union rightKeys newRightKeys
        
        return ZSet.union antiJoinResult negativeResults
    }
    
    member _.Reset() =
        leftState <- IndexedZSet.empty
        rightKeys <- Set.empty
    
    interface IBinaryOperator<ZSet<'K * 'V1>, ZSet<'K * 'V2>, ZSet<'K * 'V1>> with
        member this.EvalAsync left right = this.EvalAsyncImpl left right
        member _.InputPreferences = (NoPreference, NoPreference)

/// Cross join operator - cartesian product of all records
type CrossJoinOperator<'V1, 'V2 when 'V1: comparison and 'V2: comparison>(?name: string) =
    inherit BaseBinaryOperator<ZSet<'V1>, ZSet<'V2>, ZSet<'V1 * 'V2>>(
        defaultArg name "CrossJoin")
    
    let mutable leftState: ZSet<'V1> = ZSet.empty
    let mutable rightState: ZSet<'V2> = ZSet.empty
    
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    override _.EvalAsyncImpl leftDelta rightDelta = task {
        // Incremental cross join: δ(L × R) = (δL × R) + (L × δR) + (δL × δR)
        
        // δL × R
        let deltaL_R = 
            ZSet.toSeq leftDelta
            |> Seq.collect (fun (v1, w1) ->
                ZSet.toSeq rightState
                |> Seq.map (fun (v2, w2) -> ((v1, v2), w1 * w2)))
            |> ZSet.ofSeq
        
        // L × δR
        let L_deltaR = 
            ZSet.toSeq leftState
            |> Seq.collect (fun (v1, w1) ->
                ZSet.toSeq rightDelta
                |> Seq.map (fun (v2, w2) -> ((v1, v2), w1 * w2)))
            |> ZSet.ofSeq
        
        // δL × δR
        let deltaL_deltaR = 
            ZSet.toSeq leftDelta
            |> Seq.collect (fun (v1, w1) ->
                ZSet.toSeq rightDelta
                |> Seq.map (fun (v2, w2) -> ((v1, v2), w1 * w2)))
            |> ZSet.ofSeq
        
        // Update state
        leftState <- ZSet.union leftState leftDelta
        rightState <- ZSet.union rightState rightDelta
        
        return deltaL_R |> ZSet.union L_deltaR |> ZSet.union deltaL_deltaR
    }
    
    member _.Reset() =
        leftState <- ZSet.empty
        rightState <- ZSet.empty
    
    interface IBinaryOperator<ZSet<'V1>, ZSet<'V2>, ZSet<'V1 * 'V2>> with
        member this.EvalAsync left right = this.EvalAsyncImpl left right
        member _.InputPreferences = (NoPreference, NoPreference)

/// Convenience functions for creating complex join operators
module ComplexJoins =
    
    /// Create left outer join operator
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let inline leftOuterJoin<'K, 'V1, 'V2 when 'K: comparison and 'V1: comparison and 'V2: comparison> () = 
        new LeftOuterJoinOperator<'K, 'V1, 'V2>()
    
    /// Create right outer join operator
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let inline rightOuterJoin<'K, 'V1, 'V2 when 'K: comparison and 'V1: comparison and 'V2: comparison> () = 
        new RightOuterJoinOperator<'K, 'V1, 'V2>()
    
    /// Create full outer join operator
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let inline fullOuterJoin<'K, 'V1, 'V2 when 'K: comparison and 'V1: comparison and 'V2: comparison> () = 
        new FullOuterJoinOperator<'K, 'V1, 'V2>()
    
    /// Create semi-join operator
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let inline semiJoin<'K, 'V1, 'V2 when 'K: comparison and 'V1: comparison and 'V2: comparison> () = 
        new SemiJoinOperator<'K, 'V1, 'V2>()
    
    /// Create anti-join operator
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let inline antiJoin<'K, 'V1, 'V2 when 'K: comparison and 'V1: comparison and 'V2: comparison> () = 
        new AntiJoinOperator<'K, 'V1, 'V2>()
    
    /// Create cross join operator
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let inline crossJoin<'V1, 'V2 when 'V1: comparison and 'V2: comparison> () = 
        new CrossJoinOperator<'V1, 'V2>()