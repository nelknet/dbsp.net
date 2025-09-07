/// Indexed Z-Sets for efficient GROUP BY and JOIN operations
/// Using HashMap for O(1) index operations instead of O(log N) Map
module DBSP.Core.IndexedZSet

open FSharp.Data.Adaptive
open DBSP.Core.ZSet

/// Indexed Z-Set type using HashMap for O(1) index operations
type IndexedZSet<'K, 'V when 'K: comparison and 'V: comparison> = 
    { 
        /// Index mapping keys to ZSets of values
        Index: HashMap<'K, ZSet<'V>> 
    }
    
    // SRTP-compatible static members with O(1) HashMap operations
    static member Zero : IndexedZSet<'K, 'V> = { Index = HashMap.empty }
    
    static member (+) (indexed1: IndexedZSet<'K, 'V>, indexed2: IndexedZSet<'K, 'V>) : IndexedZSet<'K, 'V> =
        // O(N + M) HashMap union - much faster than O(N * log M) Map operations
        let combinedIndex = 
            HashMap.unionWith (fun _ zset1 zset2 -> 
                ZSet.add zset1 zset2
            ) indexed1.Index indexed2.Index
        { Index = combinedIndex }
    
    static member (~-) (indexed: IndexedZSet<'K, 'V>) : IndexedZSet<'K, 'V> = 
        let negatedIndex = HashMap.map (fun _ zset -> ZSet.negate zset) indexed.Index
        { Index = negatedIndex }

    /// Check if IndexedZSet is empty
    member this.IsEmpty = 
        HashMap.isEmpty this.Index ||
        HashMap.forall (fun _ (zset: ZSet<'V>) -> zset.IsEmpty) this.Index

    /// Get ZSet for a specific key
    member this.GetZSet(key: 'K) =
        HashMap.tryFind key this.Index |> Option.defaultValue (ZSet.empty<'V>)

    /// Get all index keys
    member this.IndexKeys =
        HashMap.keys this.Index

    /// Total count across all indexed ZSets
    member this.TotalCount =
        HashMap.fold (fun acc _ (zset: ZSet<'V>) -> acc + zset.Count) 0 this.Index

/// Module functions for IndexedZSet operations
module IndexedZSet =

    /// Create empty IndexedZSet
    let empty<'K, 'V when 'K: comparison and 'V: comparison> : IndexedZSet<'K, 'V> = 
        { Index = HashMap.empty }

    /// Create IndexedZSet from a ZSet using a key function (GROUP BY operation)
    let groupBy (keyFn: 'T -> 'K) (zset: ZSet<'T>) : IndexedZSet<'K, 'T> =
        // O(N) grouping operation using HashMap.fold for efficiency
        let grouped = 
            ZSet.fold (fun acc value weight ->
                let key = keyFn value
                let currentZSet = HashMap.tryFind key acc |> Option.defaultValue (ZSet.empty<'T>)
                let newZSet = ZSet.insert value weight currentZSet
                HashMap.add key newZSet acc
            ) HashMap.empty zset
        { Index = grouped }

    /// Create IndexedZSet from sequence of (key * value * weight) tuples
    let ofSeq (entries: seq<'K * 'V * int>) =
        entries
        |> Seq.fold (fun acc (key, value, weight) ->
            let currentZSet = HashMap.tryFind key acc |> Option.defaultValue (ZSet.empty<'V>)
            let newZSet = ZSet.insert value weight currentZSet
            HashMap.add key newZSet acc
        ) HashMap.empty
        |> fun index -> { Index = index }

    /// Convert to sequence of (key * value * weight) tuples
    let toSeq (indexed: IndexedZSet<'K, 'V>) =
        HashMap.toSeq indexed.Index
        |> Seq.collect (fun (key, zset) ->
            ZSet.toSeq zset
            |> Seq.map (fun (value, weight) -> (key, value, weight))
        )

    /// Create IndexedZSet from regular ZSet of tuples
    let fromZSet (zset: ZSet<'K * 'V>) : IndexedZSet<'K, 'V> =
        ZSet.fold (fun acc (key, value) weight ->
            let currentZSet = HashMap.tryFind key acc |> Option.defaultValue (ZSet.empty<'V>)
            let newZSet = ZSet.insert value weight currentZSet
            HashMap.add key newZSet acc
        ) HashMap.empty zset
        |> fun index -> { Index = index }

    /// Convert IndexedZSet back to regular ZSet of tuples
    let toZSet (indexed: IndexedZSet<'K, 'V>) : ZSet<'K * 'V> =
        HashMap.fold (fun acc key zset ->
            ZSet.fold (fun acc2 value weight ->
                ZSet.insert (key, value) weight acc2
            ) acc zset
        ) (ZSet.empty<'K * 'V>) indexed.Index

    /// Basic inner join of two IndexedZSets on their keys
    let join (left: IndexedZSet<'K, 'V1>) (right: IndexedZSet<'K, 'V2>) : IndexedZSet<'K, 'V1 * 'V2> =
        let joinedIndex = 
            HashMap.choose (fun key leftZSet ->
                match HashMap.tryFind key right.Index with
                | Some rightZSet ->
                    // Cartesian product of the two ZSets for this key
                    let product = 
                        ZSet.fold (fun acc1 leftVal leftWeight ->
                            ZSet.fold (fun acc2 rightVal rightWeight ->
                                let combinedWeight = leftWeight * rightWeight
                                ZSet.insert (leftVal, rightVal) combinedWeight acc2
                            ) acc1 rightZSet
                        ) (ZSet.empty<'V1 * 'V2>) leftZSet
                    if product.IsEmpty then None else Some product
                | None -> None
            ) left.Index
        { Index = joinedIndex }

    /// Filter IndexedZSet by key predicate
    let filterByKey predicate indexed =
        { Index = HashMap.filter (fun key _ -> predicate key) indexed.Index }

    /// Filter IndexedZSet by value predicate (applied to all values in all ZSets)
    let filterByValue predicate indexed =
        let filteredIndex = 
            HashMap.map (fun _ zset -> 
                ZSet.filter predicate zset
            ) indexed.Index
        { Index = filteredIndex }

    /// Map over the index keys
    let mapKeys f indexed =
        HashMap.toSeq indexed.Index
        |> Seq.map (fun (key, zset) -> (f key, zset))
        |> HashMap.ofSeq
        |> fun newIndex -> { Index = newIndex }

    /// Convenient inline functions
    let inline add (indexed1: IndexedZSet<'K,'V>) (indexed2: IndexedZSet<'K,'V>) = indexed1 + indexed2
    let inline negate (indexed: IndexedZSet<'K,'V>) = -indexed