/// Join operators for DBSP circuits with incremental state management
/// Implements incremental join semantics: δ(R ⋈ S) = (δR ⋈ S) + (R ⋈ δS) + (δR ⋈ δS)
module DBSP.Operators.JoinOperators

open System.Threading.Tasks
open DBSP.Core.ZSet
open DBSP.Core.IndexedZSet
open DBSP.Operators.Interfaces
open FSharp.Data.Adaptive

/// Inner join operator maintaining incremental state for both inputs
/// Implements the mathematical join semantics for incremental computation
type InnerJoinOperator<'K, 'V1, 'V2 when 'K: comparison and 'V1: comparison and 'V2: comparison>(?name: string) =
    inherit BaseBinaryOperator<ZSet<'K * 'V1>, ZSet<'K * 'V2>, IndexedZSet<'K, 'V1 * 'V2>>(
        defaultArg name "InnerJoin")
    
    // Maintained state for incremental computation
    let mutable leftState: IndexedZSet<'K, 'V1> = IndexedZSet.empty
    let mutable rightState: IndexedZSet<'K, 'V2> = IndexedZSet.empty

    // Adaptive fast-path: keep per-key mutable dictionaries to avoid repeated ZSet construction
    let isAdaptive = System.String.Equals(System.Environment.GetEnvironmentVariable("DBSP_BACKEND"), "Adaptive", System.StringComparison.OrdinalIgnoreCase)
    let leftDict = System.Collections.Generic.Dictionary<'K, System.Collections.Generic.Dictionary<'V1, int>>()
    let rightDict = System.Collections.Generic.Dictionary<'K, System.Collections.Generic.Dictionary<'V2, int>>()
    let mergeIntoLeft (key: 'K) (value: 'V1) (weight: int) =
        let ok, inner = leftDict.TryGetValue key
        let innerDict = if ok then inner else let d = System.Collections.Generic.Dictionary<'V1, int>() in leftDict[key] <- d; d
        let ok2, cur = innerDict.TryGetValue value
        let nw = (if ok2 then cur else 0) + weight
        if nw = 0 then
            if ok2 then innerDict.Remove value |> ignore
        else
            innerDict[value] <- nw
    let mergeIntoRight (key: 'K) (value: 'V2) (weight: int) =
        let ok, inner = rightDict.TryGetValue key
        let innerDict = if ok then inner else let d = System.Collections.Generic.Dictionary<'V2, int>() in rightDict[key] <- d; d
        let ok2, cur = innerDict.TryGetValue value
        let nw = (if ok2 then cur else 0) + weight
        if nw = 0 then
            if ok2 then innerDict.Remove value |> ignore
        else
            innerDict[value] <- nw
    
    override _.EvalAsyncImpl leftDelta rightDelta = task {
        // Decide execution path
        let leftDeltaCount = ZSet.fold (fun s _ w -> if w <> 0 then s + 1 else s) 0 leftDelta
        let rightDeltaCount = ZSet.fold (fun s _ w -> if w <> 0 then s + 1 else s) 0 rightDelta
        updateMode leftDeltaCount rightDeltaCount

        if not isAdaptive && not useHashFastPath then
            // Generic path using IndexedZSet
            let leftIndexed = IndexedZSet.fromZSet leftDelta
            let rightIndexed = IndexedZSet.fromZSet rightDelta
            let deltaLeft_State = IndexedZSet.join leftIndexed rightState
            let state_DeltaRight = IndexedZSet.join leftState rightIndexed
            let deltaLeft_DeltaRight = IndexedZSet.join leftIndexed rightIndexed
            leftState <- IndexedZSet.add leftState leftIndexed
            rightState <- IndexedZSet.add rightState rightIndexed
            let intermediate = IndexedZSet.add deltaLeft_State state_DeltaRight
            let finalResult = IndexedZSet.add intermediate deltaLeft_DeltaRight
            return finalResult
        else
            // Adaptive fast path: group deltas and join against mutable state dictionaries
            // Group left delta
            let lgrp = System.Collections.Generic.Dictionary<'K, System.Collections.Generic.List<'V1 * int>>()
            ZSet.iter (fun (k, v1) w ->
                if w <> 0 then
                    let ok, lst = lgrp.TryGetValue k
                    let l = if ok then lst else let nl = System.Collections.Generic.List<'V1 * int>() in lgrp[k] <- nl; nl
                    l.Add((v1, w))
            ) leftDelta
            // Group right delta
            let rgrp = System.Collections.Generic.Dictionary<'K, System.Collections.Generic.List<'V2 * int>>()
            ZSet.iter (fun (k, v2) w ->
                if w <> 0 then
                    let ok, lst = rgrp.TryGetValue k
                    let l = if ok then lst else let nl = System.Collections.Generic.List<'V2 * int>() in rgrp[k] <- nl; nl
                    l.Add((v2, w))
            ) rightDelta

            // Build result per key using builders
            let builders = System.Collections.Generic.Dictionary<'K, ZSet.ZSetBuilder<'V1 * 'V2>>()
            let inline getBuilder k = let ok, b = builders.TryGetValue k in if ok then b else let nb = ZSet.ZSetBuilder<'V1 * 'V2>() in builders[k] <- nb; nb

            // δR ⋈ S
            for KeyValue(k, lstL) in lgrp do
                let ok, rstate = rightDict.TryGetValue k
                if ok then
                    let b = getBuilder k
                    for (v1, lw) in lstL do
                        if lw <> 0 then
                            for KeyValue(v2, rw) in rstate do
                                if rw <> 0 then b.Add((v1, v2), lw * rw)

            // R ⋈ δS
            for KeyValue(k, lstR) in rgrp do
                let ok, lstate = leftDict.TryGetValue k
                if ok then
                    let b = getBuilder k
                    for (v2, rw) in lstR do
                        if rw <> 0 then
                            for KeyValue(v1, lw) in lstate do
                                if lw <> 0 then b.Add((v1, v2), lw * rw)

            // δR ⋈ δS
            for KeyValue(k, lstL) in lgrp do
                let ok, lstR = rgrp.TryGetValue k
                if ok then
                    let b = getBuilder k
                    for (v1, lw) in lstL do
                        if lw <> 0 then
                            for (v2, rw) in lstR do
                                if rw <> 0 then b.Add((v1, v2), lw * rw)

            // Update state dictionaries
            for KeyValue(k, lstL) in lgrp do for (v1, lw) in lstL do mergeIntoLeft k v1 lw
            for KeyValue(k, lstR) in rgrp do for (v2, rw) in lstR do mergeIntoRight k v2 rw

            // Also update the generic IndexedZSet states lazily via from dictionary (for downstream ops that expect it)
            // We keep using previous states for non-adaptive consumers; Adaptive consumers benefit from dict path already.
            let leftIdxDelta =
                if lgrp.Count = 0 then IndexedZSet.empty
                else
                    let seqLeft = seq {
                        for KeyValue(k, lst) in lgrp do
                            for (v1, lw) in lst do yield (k, v1, lw)
                    }
                    IndexedZSet.ofSeq seqLeft
            let rightIdxDelta =
                if rgrp.Count = 0 then IndexedZSet.empty
                else
                    let seqRight = seq {
                        for KeyValue(k, lst) in rgrp do
                            for (v2, rw) in lst do yield (k, v2, rw)
                    }
                    IndexedZSet.ofSeq seqRight
            leftState <- IndexedZSet.add leftState leftIdxDelta
            rightState <- IndexedZSet.add rightState rightIdxDelta

            // Materialize final IndexedZSet from builders
            let index =
                builders
                |> Seq.map (fun kv -> kv.Key, kv.Value.Build())
                |> HashMap.ofSeq
            return { IndexedZSet.Index = index }
    }
    
    interface IStatefulOperator<IndexedZSet<'K, 'V1> * IndexedZSet<'K, 'V2>> with
        member _.GetState() = (leftState, rightState)
        member _.SetState((left, right)) = 
            leftState <- left
            rightState <- right
        member _.SerializeState() = task {
            // Simple serialization - in production, use efficient binary format
            let leftSeq = IndexedZSet.toSeq leftState |> Seq.toArray
            let rightSeq = IndexedZSet.toSeq rightState |> Seq.toArray
            let stateStr = sprintf "%A|%A" leftSeq rightSeq
            return System.Text.Encoding.UTF8.GetBytes(stateStr)
        }
        member _.DeserializeState(data: byte[]) = task {
            // Simple deserialization - parse back from string format
            let stateStr = System.Text.Encoding.UTF8.GetString(data)
            let parts = stateStr.Split('|')
            if parts.Length = 2 then
                // This is simplified - production would use proper binary format
                leftState <- IndexedZSet.empty // Reset to empty for now
                rightState <- IndexedZSet.empty
            return ()
        }

/// Left join operator - includes all records from left input, nulls for missing right
type LeftJoinOperator<'K, 'V1, 'V2 when 'K: comparison and 'V1: comparison and 'V2: comparison>(?name: string) =
    inherit BaseBinaryOperator<ZSet<'K * 'V1>, ZSet<'K * 'V2>, ZSet<'K * 'V1 * 'V2 option>>(
        defaultArg name "LeftJoin")
    
    let mutable leftState: IndexedZSet<'K, 'V1> = IndexedZSet.empty
    let mutable rightState: IndexedZSet<'K, 'V2> = IndexedZSet.empty
    
    override _.EvalAsyncImpl leftDelta rightDelta = task {
        let leftIndexed = IndexedZSet.fromZSet leftDelta
        let rightIndexed = IndexedZSet.fromZSet rightDelta
        
        // For left join, we need to include all left records
        // First, do the regular join for matches
        let matchedJoin = IndexedZSet.join leftIndexed rightState
        let matchedWithSome = 
            IndexedZSet.toZSet matchedJoin
            |> ZSet.mapKeys (fun (k, (v1, v2)) -> (k, v1, Some v2))
        
        // Then add unmatched left records with None for right
        let leftOnlyRecords = 
            ZSet.fold (fun acc (key, value) weight ->
                let hasRightMatch = not (rightState.GetZSet(key).IsEmpty)
                if hasRightMatch then
                    acc // Skip matched records
                else
                    ZSet.insert (key, value, None) weight acc
            ) (ZSet.empty<'K * 'V1 * 'V2 option>) leftDelta
        
        // Update state
        leftState <- IndexedZSet.add leftState leftIndexed
        rightState <- IndexedZSet.add rightState rightIndexed
        
        // Combine matched and unmatched results
        return ZSet.add matchedWithSome leftOnlyRecords
    }

/// Semi-join operator - returns left records that have matches in right
type SemiJoinOperator<'K, 'V1, 'V2 when 'K: comparison and 'V1: comparison and 'V2: comparison>(?name: string) =
    inherit BaseBinaryOperator<ZSet<'K * 'V1>, ZSet<'K * 'V2>, ZSet<'K * 'V1>>(
        defaultArg name "SemiJoin")
    
    let mutable rightKeys: Set<'K> = Set.empty
    
    override _.EvalAsyncImpl leftDelta rightDelta = task {
        // Update set of right keys
        ZSet.fold (fun acc (key, _) weight ->
            if weight > 0 then
                Set.add key acc
            else
                Set.remove key acc  // Handle deletions
        ) rightKeys rightDelta |> fun newKeys -> rightKeys <- newKeys
        
        // Filter left records to only those with matching keys in right
        let result = 
            ZSet.fold (fun acc (key, value) weight ->
                if Set.contains key rightKeys then
                    ZSet.insert (key, value) weight acc
                else
                    acc
            ) (ZSet.empty<'K * 'V1>) leftDelta
        
        return result
    }

/// Anti-join operator - returns left records that have NO matches in right
type AntiJoinOperator<'K, 'V1, 'V2 when 'K: comparison and 'V1: comparison and 'V2: comparison>(?name: string) =
    inherit BaseBinaryOperator<ZSet<'K * 'V1>, ZSet<'K * 'V2>, ZSet<'K * 'V1>>(
        defaultArg name "AntiJoin")
    
    let mutable rightKeys: Set<'K> = Set.empty
    
    override _.EvalAsyncImpl leftDelta rightDelta = task {
        // Update set of right keys  
        ZSet.fold (fun acc (key, _) weight ->
            if weight > 0 then
                Set.add key acc
            else
                Set.remove key acc
        ) rightKeys rightDelta |> fun newKeys -> rightKeys <- newKeys
        
        // Filter left records to only those WITHOUT matching keys in right
        let result = 
            ZSet.fold (fun acc (key, value) weight ->
                if not (Set.contains key rightKeys) then
                    ZSet.insert (key, value) weight acc
                else
                    acc
            ) (ZSet.empty<'K * 'V1>) leftDelta
        
        return result
    }

/// Module functions for creating join operators
module JoinOperators =

    /// Create an inner join operator
    let innerJoin<'K, 'V1, 'V2 when 'K: comparison and 'V1: comparison and 'V2: comparison> 
        : IBinaryOperator<ZSet<'K * 'V1>, ZSet<'K * 'V2>, IndexedZSet<'K, 'V1 * 'V2>> =
        InnerJoinOperator() :> IBinaryOperator<ZSet<'K * 'V1>, ZSet<'K * 'V2>, IndexedZSet<'K, 'V1 * 'V2>>

    /// Create a left join operator
    let leftJoin<'K, 'V1, 'V2 when 'K: comparison and 'V1: comparison and 'V2: comparison> 
        : IBinaryOperator<ZSet<'K * 'V1>, ZSet<'K * 'V2>, ZSet<'K * 'V1 * 'V2 option>> =
        LeftJoinOperator() :> IBinaryOperator<ZSet<'K * 'V1>, ZSet<'K * 'V2>, ZSet<'K * 'V1 * 'V2 option>>

    /// Create a semi-join operator  
    let semiJoin<'K, 'V1, 'V2 when 'K: comparison and 'V1: comparison and 'V2: comparison>
        : IBinaryOperator<ZSet<'K * 'V1>, ZSet<'K * 'V2>, ZSet<'K * 'V1>> =
        SemiJoinOperator() :> IBinaryOperator<ZSet<'K * 'V1>, ZSet<'K * 'V2>, ZSet<'K * 'V1>>
    // Hybrid dispatch: runtime choice between hash-overlay path and generic IndexedZSet path
    // Maintain EWMAs of delta sizes and a simple hysteresis to avoid thrashing
    let inline ewma (alpha: float) (prev: float) (sample: float) = alpha * sample + (1.0 - alpha) * prev
    let alpha = 0.3
    let mutable ewmaLeft = 0.0
    let mutable ewmaRight = 0.0
    let mutable useHashFastPath = true
    // Hysteresis thresholds (tune as needed)
    // If deltas are small, prefer hash fast path; if large, fall back to generic (arranged-friendly)
    let inline updateMode (leftCount: int) (rightCount: int) =
        ewmaLeft <- ewma alpha ewmaLeft (float leftCount)
        ewmaRight <- ewma alpha ewmaRight (float rightCount)
        let avg = (ewmaLeft + ewmaRight) * 0.5
        // thresholds with hysteresis gap
        let lowThresh = 200.0
        let highThresh = 800.0
        if useHashFastPath && avg > highThresh then useHashFastPath <- false
        elif (not useHashFastPath) && avg < lowThresh then useHashFastPath <- true


    /// Create an anti-join operator
    let antiJoin<'K, 'V1, 'V2 when 'K: comparison and 'V1: comparison and 'V2: comparison>
        : IBinaryOperator<ZSet<'K * 'V1>, ZSet<'K * 'V2>, ZSet<'K * 'V1>> =
        AntiJoinOperator() :> IBinaryOperator<ZSet<'K * 'V1>, ZSet<'K * 'V2>, ZSet<'K * 'V1>>
