/// Z-Sets: Collections with multiplicities (weights) supporting
/// incremental computation through positive and negative weights
module DBSP.Core.ZSet

open System
open System.Runtime.CompilerServices

// FastZSet: robin-hood hash-based dictionary for ZSet weights
// Moved from experimental benchmarks into core for evaluation
module Collections =
    module FastZSet =
        module private Helpers =
            [<Literal>]
            let POSITIVE_INT_MASK = 0x7FFF_FFFF
            [<RequireQualifiedAccess>]
            module HashCode =
                let empty = -2
                let tombstone = -1

        [<Struct>]
        type ZSetBucket<'K when 'K : equality> = {
            mutable HashCode: int
            mutable Distance: byte
            mutable Key: 'K
            mutable Weight: int
        } with
            member this.IsEmpty = this.HashCode = Helpers.HashCode.empty
            member this.IsTombstone = this.HashCode = Helpers.HashCode.tombstone
            member this.IsOccupied = this.HashCode >= Helpers.HashCode.tombstone
            member this.IsValid = this.HashCode >= 0
            static member Empty = {
                HashCode = Helpers.HashCode.empty
                Distance = 0uy
                Key = Unchecked.defaultof<'K>
                Weight = 0
            }

        type FastZSet<'K when 'K : equality> = {
            mutable Buckets: ZSetBucket<'K>[]
            mutable Count: int
            mutable Mask: int
            Comparer: Collections.Generic.IEqualityComparer<'K>
        } with
            member this.IsEmpty = this.Count = 0
            member this.Capacity = this.Buckets.Length

        module private StringHasher =
            let stringComparer =
                { new Collections.Generic.IEqualityComparer<string> with
                    member _.Equals(a: string, b: string) = String.Equals(a, b, StringComparison.Ordinal)
                    member _.GetHashCode(str: string) = if isNull str then 0 else String.GetHashCode(str, StringComparison.Ordinal) }

        [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
        let empty<'K when 'K : equality> (capacity: int) : FastZSet<'K> =
            let actualCapacity = if capacity <= 0 then 16 else 1 <<< (32 - System.Numerics.BitOperations.LeadingZeroCount(uint32 (capacity - 1)))
            let comparer : Collections.Generic.IEqualityComparer<'K> =
                if typeof<'K> = typeof<string> then Unchecked.unbox StringHasher.stringComparer
                else System.Collections.Generic.EqualityComparer<'K>.Default
            { Buckets = Array.create actualCapacity ZSetBucket.Empty; Count = 0; Mask = actualCapacity - 1; Comparer = comparer }

        [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
        let create<'K when 'K : equality>() = empty<'K> 16

        [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
        let private findBucket (dict: FastZSet<'K>) (key: 'K) (hash: int) =
            let mutable index = hash &&& dict.Mask
            let mutable distance = 0uy
            let mutable found = false
            while not found && distance <= 255uy do
                let bucket = &dict.Buckets.[index]
                if bucket.IsEmpty then
                    found <- true; index <- -1
                elif bucket.IsValid && bucket.HashCode = hash && dict.Comparer.Equals(bucket.Key, key) then
                    found <- true
                elif bucket.IsValid && byte distance > bucket.Distance then
                    found <- true; index <- -1
                else
                    distance <- distance + 1uy
                    index <- (index + 1) &&& dict.Mask
            if found then index else -1

        [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
        let rec insertOrUpdate (dict: FastZSet<'K>) (key: 'K) (weight: int) =
            let hash = dict.Comparer.GetHashCode(key) &&& Helpers.POSITIVE_INT_MASK
            let bucketIndex = findBucket dict key hash
            if bucketIndex >= 0 then
                let bucket = &dict.Buckets.[bucketIndex]
                bucket.Weight <- bucket.Weight + weight
                if bucket.Weight = 0 then
                    bucket.HashCode <- Helpers.HashCode.tombstone
                    dict.Count <- dict.Count - 1
            elif weight <> 0 then
                // ensure capacity before insertion
                let load = float dict.Count / float dict.Buckets.Length
                if load > 0.85 then
                    // simple rehash to double capacity
                    let old = dict.Buckets
                    let newCap = dict.Buckets.Length <<< 1
                    dict.Buckets <- Array.create newCap ZSetBucket.Empty
                    dict.Mask <- newCap - 1
                    dict.Count <- 0
                    for b in old do
                        if b.IsValid then
                            insertOrUpdate dict b.Key b.Weight
                // Robin Hood insertion with stealing
                let mutable idx = hash &&& dict.Mask
                let mutable d = 0uy
                let mutable k = key
                let mutable w = weight
                let mutable h = hash
                let mutable inserted = false
                while not inserted do
                    let b = &dict.Buckets.[idx]
                    if b.IsEmpty || b.IsTombstone then
                        b.HashCode <- h; b.Key <- k; b.Weight <- w; b.Distance <- d
                        dict.Count <- dict.Count + 1
                        inserted <- true
                    elif b.IsValid && b.HashCode = h && dict.Comparer.Equals(b.Key, k) then
                        b.Weight <- b.Weight + w
                        if b.Weight = 0 then
                            b.HashCode <- Helpers.HashCode.tombstone
                            dict.Count <- dict.Count - 1
                        inserted <- true
                    elif b.Distance < d then
                        let tmpH, tmpD, tmpK, tmpW = b.HashCode, b.Distance, b.Key, b.Weight
                        b.HashCode <- h; b.Distance <- d; b.Key <- k; b.Weight <- w
                        h <- tmpH; d <- tmpD; k <- tmpK; w <- tmpW
                        idx <- (idx + 1) &&& dict.Mask; d <- d + 1uy
                    else
                        idx <- (idx + 1) &&& dict.Mask; d <- d + 1uy

        let ofSeq (pairs: seq<'K * int>) =
            let dict = empty<'K> 16
            for (key, weight) in pairs do if weight <> 0 then insertOrUpdate dict key weight
            dict

        [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
        let tryGetWeight (dict: FastZSet<'K>) (key: 'K) =
            let hash = dict.Comparer.GetHashCode(key) &&& Helpers.POSITIVE_INT_MASK
            let bucketIndex = findBucket dict key hash
            if bucketIndex >= 0 then dict.Buckets.[bucketIndex].Weight else 0

        [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
        let ofList (pairs: ('K * int) list) = ofSeq pairs

        let union (dict1: FastZSet<'K>) (dict2: FastZSet<'K>) =
            let result = empty<'K> (max dict1.Capacity dict2.Capacity)
            for bucket in dict1.Buckets do if bucket.IsValid then insertOrUpdate result bucket.Key bucket.Weight
            for bucket in dict2.Buckets do if bucket.IsValid then insertOrUpdate result bucket.Key bucket.Weight
            result

        let toSeq (dict: FastZSet<'K>) = seq { for bucket in dict.Buckets do if bucket.IsValid then yield (bucket.Key, bucket.Weight) }

module FZ = Collections.FastZSet

/// Z-set type backed by FastZSet storage (equality-constrained keys)
type ZSet<'K when 'K : equality> =
    {
        Inner: FZ.FastZSet<'K>
    }

// Algebraic operations
type ZSet<'K when 'K : equality> with
    static member Zero : ZSet<'K> = { Inner = FZ.empty 0 }

    static member (+) (zset1: ZSet<'K>, zset2: ZSet<'K>) : ZSet<'K> =
        { Inner = FZ.union zset1.Inner zset2.Inner }

    static member (~-) (zset: ZSet<'K>) : ZSet<'K> =
        let res = FZ.empty<'K> zset.Inner.Capacity
        for (k, w) in FZ.toSeq zset.Inner do
            if w <> 0 then FZ.insertOrUpdate res k (-w)
        { Inner = res }

    static member (*) (scalar: int, zset: ZSet<'K>) : ZSet<'K> =
        if scalar = 1 then zset
        elif scalar = 0 then { Inner = FZ.empty 0 }
        else
            let res = FZ.empty<'K> zset.Inner.Capacity
            for (k, w) in FZ.toSeq zset.Inner do
                let nw = scalar * w
                if nw <> 0 then FZ.insertOrUpdate res k nw
            { Inner = res }

    /// Check if the ZSet is empty (contains no elements with non-zero weight)
    member this.IsEmpty = this.Inner.Count = 0

    /// Get the weight of a specific key
    member this.GetWeight(key: 'K) = FZ.tryGetWeight this.Inner key

    /// Get all keys with non-zero weights
    member this.Keys = FZ.toSeq this.Inner |> Seq.map fst

    /// Get total number of elements (sum of absolute weights)
    member this.Count = FZ.toSeq this.Inner |> Seq.fold (fun acc (_, w) -> acc + abs w) 0

/// Module functions for ZSet operations
module ZSet =

    /// Create empty ZSet
    let empty<'K when 'K : equality> : ZSet<'K> = { Inner = FZ.empty 0 }

    /// Create singleton ZSet with given key and weight
    let singleton key weight =
        let d = FZ.empty 1
        if weight <> 0 then FZ.insertOrUpdate d key weight
        { Inner = d }

    /// Create ZSet from sequence of key-weight pairs
    let ofSeq (pairs: seq<'K * int>) = { Inner = FZ.ofSeq pairs }

    /// Create ZSet from list of key-weight pairs
    let ofList (pairs: ('K * int) list) = { Inner = FZ.ofList pairs }

    /// Efficient batch insertion
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let insertMany (zset: ZSet<'K>) (pairs: seq<'K * int>) =
        let additions = FZ.ofSeq pairs
        { Inner = FZ.union zset.Inner additions }

    /// Builder for efficient ZSet construction
    type ZSetBuilder<'K when 'K : equality>() =
        let dict = FZ.empty<'K> 16
        member _.Add(key: 'K, weight: int) = if weight <> 0 then FZ.insertOrUpdate dict key weight
        member this.AddRange(pairs: seq<'K * int>) = for (k,w) in pairs do this.Add(k,w)
        member _.Build() = { Inner = dict }

    /// Build ZSet efficiently using builder pattern
    let buildZSet (builderFn: ZSetBuilder<'K> -> unit) =
        let builder = ZSetBuilder<'K>()
        builderFn builder
        builder.Build()

    /// Optimized builder entry used by fused operators and benchmarks
    let buildWith (builderFn: ZSetBuilder<'K> -> unit) = buildZSet builderFn

    /// Convert ZSet to sequence of key-weight pairs (excluding zero weights)
    let toSeq (zset: ZSet<'K>) = FZ.toSeq zset.Inner

    /// Convenient inline functions using F# 7+ simplified SRTP syntax
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let inline add (zset1: ZSet<'K>) (zset2: ZSet<'K>) = zset1 + zset2

    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let inline negate (zset: ZSet<'K>) = -zset

    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let inline scalarMultiply scalar (zset: ZSet<'K>) = scalar * zset

    /// Insert a key with given weight (adds to existing weight)
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let inline insert key weight (zset: ZSet<'K>) =
        // Build a tiny delta and union
        let d = FZ.empty 1
        if weight <> 0 then FZ.insertOrUpdate d key weight
        { Inner = FZ.union zset.Inner d }

    /// Remove a key by adding negative weight
    let remove key weight zset = insert key (-weight) zset

    /// Union of two ZSets (same as addition)
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let inline union zset1 zset2 = add zset1 zset2

    /// Difference of two ZSets
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let inline difference zset1 zset2 = add zset1 (negate zset2)

    /// Filter ZSet by key predicate
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let inline filter ([<InlineIfLambda>] predicate) zset =
        buildWith (fun b ->
            for (k,w) in toSeq zset do
                if w <> 0 && predicate k then b.Add(k, w)
        )

    /// Map over keys (preserving weights)
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let inline mapKeys ([<InlineIfLambda>] f) zset =
        toSeq zset |> Seq.map (fun (k,w) -> (f k, w)) |> ofSeq

    /// Fold over ZSet entries
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let inline fold ([<InlineIfLambda>] folder) state zset =
        (toSeq zset) |> Seq.fold (fun acc (k,w) -> if w <> 0 then folder acc k w else acc) state
