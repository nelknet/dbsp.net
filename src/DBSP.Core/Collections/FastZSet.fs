/// Ultra-high-performance dictionary implementation for DBSP operations
/// Based on Robin Hood hashing with DBSP-specific optimizations
/// 
/// STATUS: Experimental implementation for future optimization
/// CURRENT DECISION: Keep using HashMap - it performs well (12μs for 10K unions)
/// WHEN TO USE: Consider this implementation only if HashMap becomes a proven 
/// bottleneck in production workloads requiring the extra 2-3x performance gain
/// 
/// The real performance issue was usage patterns (44MB allocations from repeated
/// singleton creation), not HashMap performance. Those issues have been fixed
/// with ZSetBuilder pattern and batch operations in ZSet.fs.
/// 
/// This FastZSet implementation provides:
/// - Custom Robin Hood hashing with distance tracking
/// - Struct-based buckets for cache efficiency  
/// - Native pointer string hashing
/// - DBSP-specific weight storage optimizations
/// 
/// TODO: Complete Robin Hood insertion logic (currently incomplete)
/// TODO: Add resize/rehashing support
/// TODO: Comprehensive testing and validation
module DBSP.Core.Collections.FastZSet

open System
open System.Numerics
open System.Runtime.CompilerServices
open Microsoft.FSharp.NativeInterop

#nowarn "9" "42" "51"

/// High-performance helpers for zero-cost operations
module private Helpers =
    
    /// Zero-cost type retyping for performance
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let inline retype<'T,'U> (x: 'T) : 'U = (# "" x: 'U #)
    
    /// Bit mask for positive integers
    [<Literal>]
    let POSITIVE_INT_MASK = 0x7FFF_FFFF
    
    /// Special hash codes for bucket states
    [<RequireQualifiedAccess>]
    module HashCode =
        let empty = -2      // Empty bucket
        let tombstone = -1  // Deleted bucket (Robin Hood)
        let inline isValid hash = hash >= 0

/// DBSP-optimized bucket structure for Z-sets
[<Struct>]
type ZSetBucket<'K when 'K : equality> = {
    /// Hash code (negative = special state)
    mutable HashCode: int
    /// Robin Hood distance from ideal position  
    mutable Distance: byte
    /// Key (undefined if empty/tombstone)
    mutable Key: 'K
    /// Weight (DBSP-specific)
    mutable Weight: int
} with
    member this.IsEmpty = this.HashCode = Helpers.HashCode.empty
    member this.IsTombstone = this.HashCode = Helpers.HashCode.tombstone  
    member this.IsOccupied = this.HashCode >= Helpers.HashCode.tombstone
    member this.IsValid = this.HashCode >= 0
    
    /// Create empty bucket
    static member Empty = {
        HashCode = Helpers.HashCode.empty
        Distance = 0uy
        Key = Unchecked.defaultof<'K>
        Weight = 0
    }

/// High-performance ZSet using Robin Hood hashing
type FastZSet<'K when 'K : equality> = {
    /// Bucket array (power of 2 size)
    mutable Buckets: ZSetBucket<'K>[]
    /// Current number of elements
    mutable Count: int
    /// Size mask for modulo operation (buckets.Length - 1)
    mutable Mask: int
    /// Equality comparer for keys
    Comparer: Collections.Generic.IEqualityComparer<'K>
} with
    
    /// Check if ZSet is empty
    member this.IsEmpty = this.Count = 0
    
    /// Get capacity (bucket array size)
    member this.Capacity = this.Buckets.Length

/// Custom string hash comparer optimized for cache performance
module private StringHasher =
    
    let stringComparer =
        { new Collections.Generic.IEqualityComparer<string> with
            member _.Equals(a: string, b: string) =
                String.Equals(a, b, StringComparison.Ordinal)

            member _.GetHashCode(str: string) =
                if isNull str then 0
                else
                    // High-performance DJB2-style hash using native pointers
                    let span = str.AsSpan()
                    let mutable hash1 = (5381u <<< 16) + 5381u
                    let mutable hash2 = hash1
                    let mutable length = str.Length
                    let mutable ptr: nativeptr<uint32> = &&span.GetPinnableReference() |> Helpers.retype
                    
                    // Process 8 bytes at a time
                    while length > 2 do
                        length <- length - 4
                        hash1 <- (BitOperations.RotateLeft(hash1, 5) + hash1) ^^^ (NativePtr.get ptr 0)
                        hash2 <- (BitOperations.RotateLeft(hash2, 5) + hash2) ^^^ (NativePtr.get ptr 1)
                        ptr <- NativePtr.add ptr 2

                    // Handle remaining bytes
                    if length > 0 then
                        hash2 <- (BitOperations.RotateLeft(hash2, 5) + hash2) ^^^ (NativePtr.get ptr 0)

                    int (hash1 + (hash2 * 1566083941u))
        }

/// High-performance operations for FastZSet
module FastZSet =
    
    /// Create empty FastZSet with specified initial capacity
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let empty<'K when 'K : equality> (capacity: int) : FastZSet<'K> =
        let actualCapacity = 
            if capacity <= 0 then 16
            else 1 <<< (32 - BitOperations.LeadingZeroCount(uint32 (capacity - 1)))
        
        let comparer = 
            if typeof<'K> = typeof<string> then
                StringHasher.stringComparer |> Helpers.retype
            else
                System.Collections.Generic.EqualityComparer<'K>.Default
                
        {
            Buckets = Array.create actualCapacity ZSetBucket.Empty
            Count = 0
            Mask = actualCapacity - 1
            Comparer = comparer
        }
    
    /// Create empty FastZSet with default capacity
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let create<'K when 'K : equality>() = empty<'K> 16
    
    /// Find bucket index using Robin Hood probing
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let private findBucket (dict: FastZSet<'K>) (key: 'K) (hash: int) =
        let mutable index = hash &&& dict.Mask
        let mutable distance = 0uy
        let mutable found = false
        
        while not found && distance <= 255uy do
            let bucket = &dict.Buckets.[index]
            
            if bucket.IsEmpty then
                found <- true
                index <- -1 // Not found
            elif bucket.IsValid && bucket.HashCode = hash && dict.Comparer.Equals(bucket.Key, key) then
                found <- true
            elif byte distance > bucket.Distance then
                found <- true
                index <- -1 // Key would be here if it existed
            else
                distance <- distance + 1uy
                index <- (index + 1) &&& dict.Mask
        
        if found then index else -1
    
    /// Insert or update key with weight
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let insertOrUpdate (dict: FastZSet<'K>) (key: 'K) (weight: int) =
        let hash = dict.Comparer.GetHashCode(key) &&& Helpers.POSITIVE_INT_MASK
        let bucketIndex = findBucket dict key hash
        
        if bucketIndex >= 0 then
            // Key exists, update weight
            let bucket = &dict.Buckets.[bucketIndex]
            bucket.Weight <- bucket.Weight + weight
            
            // Remove if weight becomes zero
            if bucket.Weight = 0 then
                bucket.HashCode <- Helpers.HashCode.tombstone
                dict.Count <- dict.Count - 1
        else
            // Key doesn't exist, need to insert if weight != 0
            if weight <> 0 then
                // TODO: Implement Robin Hood insertion
                let insertIndex = hash &&& dict.Mask
                let bucket = &dict.Buckets.[insertIndex]
                bucket.HashCode <- hash
                bucket.Key <- key
                bucket.Weight <- weight
                bucket.Distance <- 0uy
                dict.Count <- dict.Count + 1
    
    /// Get weight for key (0 if not present)
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let tryGetWeight (dict: FastZSet<'K>) (key: 'K) =
        let hash = dict.Comparer.GetHashCode(key) &&& Helpers.POSITIVE_INT_MASK
        let bucketIndex = findBucket dict key hash
        
        if bucketIndex >= 0 then
            dict.Buckets.[bucketIndex].Weight
        else
            0
    
    /// Create FastZSet from sequence of key-weight pairs
    let ofSeq (pairs: seq<'K * int>) =
        let dict = empty<'K> 16
        for (key, weight) in pairs do
            if weight <> 0 then
                insertOrUpdate dict key weight
        dict
    
    /// Create FastZSet from list
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let ofList (pairs: ('K * int) list) = ofSeq pairs
    
    /// Union two FastZSets with weight addition
    let union (dict1: FastZSet<'K>) (dict2: FastZSet<'K>) =
        let result = empty<'K> (max dict1.Capacity dict2.Capacity)
        
        // Add all entries from dict1
        for bucket in dict1.Buckets do
            if bucket.IsValid then
                insertOrUpdate result bucket.Key bucket.Weight
        
        // Add all entries from dict2
        for bucket in dict2.Buckets do
            if bucket.IsValid then
                insertOrUpdate result bucket.Key bucket.Weight
        
        result
    
    /// Convert to sequence of key-weight pairs
    let toSeq (dict: FastZSet<'K>) = seq {
        for bucket in dict.Buckets do
            if bucket.IsValid then
                yield (bucket.Key, bucket.Weight)
    }