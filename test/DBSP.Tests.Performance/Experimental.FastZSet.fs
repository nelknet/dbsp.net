module DBSP.Core.Collections.FastZSet

open System
open System.Numerics
open System.Runtime.CompilerServices
    module private Helpers =
        [<Literal>]
        let POSITIVE_INT_MASK = 0x7FFF_FFFF
        [<RequireQualifiedAccess>]
        module HashCode =
            let empty = -2
            let tombstone = -1
            let inline isValid hash = hash >= 0

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
        let actualCapacity = if capacity <= 0 then 16 else 1 <<< (32 - BitOperations.LeadingZeroCount(uint32 (capacity - 1)))
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
            let mutable dist = 0uy
            let mutable k = key
            let mutable w = weight
            let mutable h = hash
            let mutable d = dist
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

    // Re-export into a nested module to match original API (type vs module separation)
    module FastZSet =
        [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
        let empty<'K when 'K : equality> capacity = empty<'K> capacity
        [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
        let create<'K when 'K : equality>() = create<'K>()
        [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
        let ofSeq pairs = ofSeq pairs
        [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
        let ofList pairs = ofList pairs
        [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
        let union a b = union a b
        [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
        let toSeq dict = toSeq dict
        [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
        let tryGetWeight dict key = tryGetWeight dict key
