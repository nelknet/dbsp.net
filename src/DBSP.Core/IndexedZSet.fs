/// Indexed Z-Sets for efficient GROUP BY and JOIN operations
/// Using HashMap for O(1) index operations instead of O(log N) Map
module DBSP.Core.IndexedZSet

open System.Runtime.CompilerServices
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
        // Build per-key using ZSet builders to avoid repeated inserts
        let dict = System.Collections.Generic.Dictionary<'K, ZSet.ZSetBuilder<'T>>()
        ZSet.iter (fun value weight ->
            let key = keyFn value
            let ok, b = dict.TryGetValue key
            let builder = if ok then b else let nb = ZSet.ZSetBuilder<'T>() in dict[key] <- nb; nb
            builder.Add(value, weight)
        ) zset
        let idx =
            dict
            |> Seq.map (fun kv -> kv.Key, kv.Value.Build())
            |> HashMap.ofSeq
        { Index = idx }

    /// Create IndexedZSet from sequence of (key * value * weight) tuples
    let ofSeq (entries: seq<'K * 'V * int>) =
        let dict = System.Collections.Generic.Dictionary<'K, ZSet.ZSetBuilder<'V>>()
        for (key, value, weight) in entries do
            let ok, b = dict.TryGetValue key
            let builder = if ok then b else let nb = ZSet.ZSetBuilder<'V>() in dict[key] <- nb; nb
            builder.Add(value, weight)
        let index = dict |> Seq.map (fun kv -> kv.Key, kv.Value.Build()) |> HashMap.ofSeq
        { Index = index }

    /// Convert to sequence of (key * value * weight) tuples
    let toSeq (indexed: IndexedZSet<'K, 'V>) =
        HashMap.toSeq indexed.Index
        |> Seq.collect (fun (key, zset) ->
            ZSet.toSeq zset
            |> Seq.map (fun (value, weight) -> (key, value, weight))
        )

    /// Create IndexedZSet from regular ZSet of tuples
    let fromZSet (zset: ZSet<'K * 'V>) : IndexedZSet<'K, 'V> =
        let dict = System.Collections.Generic.Dictionary<'K, ZSet.ZSetBuilder<'V>>()
        ZSet.iter (fun (key, value) weight ->
            let ok, b = dict.TryGetValue key
            let builder = if ok then b else let nb = ZSet.ZSetBuilder<'V>() in dict[key] <- nb; nb
            builder.Add(value, weight)
        ) zset
        let index = dict |> Seq.map (fun kv -> kv.Key, kv.Value.Build()) |> HashMap.ofSeq
        { Index = index }

    /// Convert IndexedZSet back to regular ZSet of tuples
    let toZSet (indexed: IndexedZSet<'K, 'V>) : ZSet<'K * 'V> =
        // Use a single builder to avoid repeated allocations
        ZSet.buildWith (fun b ->
            HashMap.iter (fun key (zset: ZSet<'V>) ->
                ZSet.iter (fun value weight -> if weight <> 0 then b.Add((key, value), weight)) zset
            ) indexed.Index
        )


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

    /// Convenient inline functions with aggressive optimization
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let inline add (indexed1: IndexedZSet<'K,'V>) (indexed2: IndexedZSet<'K,'V>) = indexed1 + indexed2
    
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let inline negate (indexed: IndexedZSet<'K,'V>) = -indexed

    /// High-performance join operation with aggressive inlining
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let join (left: IndexedZSet<'K, 'V1>) (right: IndexedZSet<'K, 'V2>) : IndexedZSet<'K, 'V1 * 'V2> =
        // For each key present in both, build the product ZSet via builder to reduce overhead
        let joinedIndex =
            HashMap.choose (fun key leftZSet ->
                match HashMap.tryFind key right.Index with
                | Some rightZSet ->
                    let product =
                        ZSet.buildWith (fun b ->
                            ZSet.iter (fun lval lw ->
                                if lw <> 0 then
                                    ZSet.iter (fun rval rw ->
                                        if rw <> 0 then b.Add((lval, rval), lw * rw)
                                    ) rightZSet
                            ) leftZSet
                        )
                    if product.IsEmpty then None else Some product
                | None -> None
            ) left.Index
        { Index = joinedIndex }
