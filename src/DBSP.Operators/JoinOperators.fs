/// Join operators for DBSP circuits with incremental state management
/// Implements incremental join semantics: δ(R ⋈ S) = (δR ⋈ S) + (R ⋈ δS) + (δR ⋈ δS)
module DBSP.Operators.JoinOperators

open System.Threading.Tasks
open DBSP.Core.ZSet
open DBSP.Core.IndexedZSet
open DBSP.Operators.Interfaces

/// Inner join operator maintaining incremental state for both inputs
/// Implements the mathematical join semantics for incremental computation
type InnerJoinOperator<'K, 'V1, 'V2 when 'K: comparison and 'V1: comparison and 'V2: comparison>(?name: string) =
    inherit BaseBinaryOperator<ZSet<'K * 'V1>, ZSet<'K * 'V2>, IndexedZSet<'K, 'V1 * 'V2>>(
        defaultArg name "InnerJoin")
    
    // Maintained state for incremental computation
    let mutable leftState: IndexedZSet<'K, 'V1> = IndexedZSet.empty
    let mutable rightState: IndexedZSet<'K, 'V2> = IndexedZSet.empty
    
    override _.EvalAsyncImpl leftDelta rightDelta = task {
        // Convert input deltas to indexed form for efficient joining
        let leftIndexed = IndexedZSet.fromZSet leftDelta
        let rightIndexed = IndexedZSet.fromZSet rightDelta
        
        // Incremental join formula: δ(R ⋈ S) = (δR ⋈ S) + (R ⋈ δS) + (δR ⋈ δS)
        
        // δR ⋈ S: Join new left data with existing right state
        let deltaLeft_State = IndexedZSet.join leftIndexed rightState
        
        // R ⋈ δS: Join existing left state with new right data  
        let state_DeltaRight = IndexedZSet.join leftState rightIndexed
        
        // δR ⋈ δS: Join new left data with new right data
        let deltaLeft_DeltaRight = IndexedZSet.join leftIndexed rightIndexed
        
        // Update maintained state for future incremental joins
        leftState <- IndexedZSet.add leftState leftIndexed
        rightState <- IndexedZSet.add rightState rightIndexed
        
        // Combine all three join results
        let intermediate = IndexedZSet.add deltaLeft_State state_DeltaRight
        let finalResult = IndexedZSet.add intermediate deltaLeft_DeltaRight
        
        return finalResult
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

    /// Create an anti-join operator
    let antiJoin<'K, 'V1, 'V2 when 'K: comparison and 'V1: comparison and 'V2: comparison>
        : IBinaryOperator<ZSet<'K * 'V1>, ZSet<'K * 'V2>, ZSet<'K * 'V1>> =
        AntiJoinOperator() :> IBinaryOperator<ZSet<'K * 'V1>, ZSet<'K * 'V2>, ZSet<'K * 'V1>>