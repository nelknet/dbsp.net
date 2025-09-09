/// Ultra-high-performance collections optimized for DBSP operations
/// Implements advanced F# performance patterns from research and benchmarking
module DBSP.Core.Collections.OptimizedCollections

open System
open System.Collections.Generic
open System.Numerics
open System.Runtime.CompilerServices
open System.Runtime.InteropServices
open Microsoft.FSharp.NativeInterop

#nowarn "9" "42" "51"

/// High-performance utilities following F# optimization patterns
module Utilities =
    
    /// Zero-cost type conversion for performance critical paths
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let inline retype<'T,'U> (x: 'T) : 'U = (# "" x: 'U #)
    
    /// Optimized hash combining using BitOperations
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let inline combineHash (h1: uint32) (h2: uint32) =
        h1 ^^^ h2 + 0x9e3779b9u + (h1 <<< 6) + (h1 >>> 2)
    
    /// DBSP-specific weight-aware hash combining
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let inline combineWeightHash (key: int) (weight: int) =
        let k = uint32 key
        let w = uint32 weight
        combineHash k w |> int
    
    /// Fast modulo operation for power-of-2 sizes
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let inline fastMod (value: int) (mask: int) = value &&& mask
    
    /// Next power of 2 using BitOperations
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let inline nextPowerOfTwo (n: int) =
        if n <= 0 then 1
        else 1 <<< (32 - BitOperations.LeadingZeroCount(uint32 (n - 1)))

/// SIMD-optimized operations for vectorizable computations
module SIMDOptimizations =
    
    /// Vectorized weight summation using System.Numerics.Vector
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let vectorizedSum (weights: int[]) =
        if weights.Length = 0 then 0
        elif weights.Length < Vector<int>.Count then
            Array.sum weights
        else
            let mutable sum = Vector<int>.Zero
            let vectorSize = Vector<int>.Count
            let vectorCount = weights.Length / vectorSize
            
            // Process full vectors
            for i in 0 .. vectorCount - 1 do
                let startIdx = i * vectorSize
                let vector = Vector<int>(weights, startIdx)
                sum <- sum + vector
            
            // Sum vector components
            let mutable total = 0
            for i in 0 .. vectorSize - 1 do
                total <- total + sum.[i]
            
            // Handle remainder
            for i in vectorCount * vectorSize .. weights.Length - 1 do
                total <- total + weights.[i]
            
            total
    
    /// SIMD-accelerated element-wise operations
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let vectorizedAdd (left: int[]) (right: int[]) =
        if left.Length <> right.Length then 
            invalidArg "arrays" "Arrays must have same length"
        
        let result = Array.zeroCreate left.Length
        let vectorSize = Vector<int>.Count
        let vectorCount = left.Length / vectorSize
        
        // Process vectors
        for i in 0 .. vectorCount - 1 do
            let startIdx = i * vectorSize
            let leftVec = Vector<int>(left, startIdx)
            let rightVec = Vector<int>(right, startIdx)
            (leftVec + rightVec).CopyTo(result, startIdx)
        
        // Handle remainder
        for i in vectorCount * vectorSize .. left.Length - 1 do
            result.[i] <- left.[i] + right.[i]
        
        result

/// Struct-based bucket for minimal allocations and optimal cache locality
[<Struct>]
[<StructLayout(LayoutKind.Sequential, Pack = 1)>]
type OptimizedBucket<'K when 'K : equality> = {
    /// Hash code (negative for special states)
    mutable Hash: int
    /// Robin Hood distance (byte to save memory)
    mutable Distance: byte
    /// Key value
    mutable Key: 'K
    /// DBSP weight
    mutable Weight: int
} with
    member this.IsEmpty = this.Hash = -2
    member this.IsTombstone = this.Hash = -1
    member this.IsOccupied = this.Hash >= -1
    member this.IsValid = this.Hash >= 0
    
    static member Empty = {
        Hash = -2
        Distance = 0uy  
        Key = Unchecked.defaultof<'K>
        Weight = 0
    }

/// Ultra-fast ZSet implementation using Robin Hood hashing with DBSP optimizations
type UltraFastZSet<'K when 'K : equality> = {
    /// Power-of-2 sized bucket array for fast modulo
    mutable Buckets: OptimizedBucket<'K>[]
    /// Current element count
    mutable Count: int
    /// Size mask (buckets.Length - 1) for fast modulo
    mutable SizeMask: int
    /// Load factor threshold (0.75)
    LoadThreshold: int
    /// Optimized equality comparer
    Comparer: IEqualityComparer<'K>
} with
    member this.IsEmpty = this.Count = 0
    member this.Capacity = this.Buckets.Length

/// High-performance ZSet operations with DBSP-specific optimizations
module UltraFastZSet =
    
    /// Create empty UltraFastZSet with specified capacity
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let create<'K when 'K : equality> (initialCapacity: int) =
        let capacity = Utilities.nextPowerOfTwo (max 16 initialCapacity)
        let comparer = EqualityComparer<'K>.Default
        
        {
            Buckets = Array.create capacity OptimizedBucket<'K>.Empty
            Count = 0
            SizeMask = capacity - 1
            LoadThreshold = capacity * 3 / 4 // 75% load factor
            Comparer = comparer
        }
    
    /// Create empty UltraFastZSet with default capacity
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let empty<'K when 'K : equality>() = create<'K> 16
    
    /// Robin Hood hash insertion with distance tracking
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let private insertRobinHood (dict: UltraFastZSet<'K>) (key: 'K) (weight: int) (hash: int) =
        let mutable index = Utilities.fastMod hash dict.SizeMask
        let mutable insertDistance = 0uy
        let mutable insertKey = key
        let mutable insertWeight = weight
        let mutable insertHash = hash
        
        let mutable inserted = false
        while not inserted do
            let bucket = &dict.Buckets.[index]
            
            if bucket.IsEmpty || bucket.IsTombstone then
                // Found empty slot, insert here
                bucket.Hash <- insertHash
                bucket.Distance <- insertDistance
                bucket.Key <- insertKey
                bucket.Weight <- insertWeight
                inserted <- true
                dict.Count <- dict.Count + 1
            elif bucket.IsValid && bucket.Hash = insertHash && dict.Comparer.Equals(bucket.Key, insertKey) then
                // Key exists, update weight
                bucket.Weight <- bucket.Weight + insertWeight
                if bucket.Weight = 0 then
                    bucket.Hash <- -1 // Mark as tombstone
                    dict.Count <- dict.Count - 1
                inserted <- true
            elif insertDistance > bucket.Distance then
                // Robin Hood: steal from the rich
                let tempHash = bucket.Hash
                let tempDistance = bucket.Distance
                let tempKey = bucket.Key
                let tempWeight = bucket.Weight
                
                bucket.Hash <- insertHash
                bucket.Distance <- insertDistance
                bucket.Key <- insertKey
                bucket.Weight <- insertWeight
                
                insertHash <- tempHash
                insertDistance <- tempDistance + 1uy
                insertKey <- tempKey
                insertWeight <- tempWeight
            else
                insertDistance <- insertDistance + 1uy
                
            index <- (index + 1) &&& dict.SizeMask
    
    /// Lookup weight for key (0 if not present)
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let tryGetWeight (dict: UltraFastZSet<'K>) (key: 'K) =
        let hash = dict.Comparer.GetHashCode(key) &&& 0x7FFF_FFFF
        let mutable index = Utilities.fastMod hash dict.SizeMask
        let mutable distance = 0uy
        let mutable found = false
        let mutable result = 0
        
        while not found && distance <= 255uy do
            let bucket = &dict.Buckets.[index]
            
            if bucket.IsEmpty then
                found <- true // Not found
            elif bucket.IsValid && bucket.Hash = hash && dict.Comparer.Equals(bucket.Key, key) then
                result <- bucket.Weight
                found <- true
            elif distance > bucket.Distance then
                found <- true // Would be here if it existed
            else
                distance <- distance + 1uy
                index <- (index + 1) &&& dict.SizeMask
        
        result
    
    /// Insert key with weight (adds to existing weight)
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let insert (dict: UltraFastZSet<'K>) (key: 'K) (weight: int) =
        if weight <> 0 then
            let hash = dict.Comparer.GetHashCode(key) &&& 0x7FFF_FFFF
            insertRobinHood dict key weight hash
    
    /// Create UltraFastZSet from list of key-weight pairs
    let ofList (pairs: ('K * int) list) =
        let dict = create<'K> (List.length pairs)
        List.iter (fun (key, weight) -> insert dict key weight) pairs
        dict
    
    /// Union two UltraFastZSets with weight addition
    let union (dict1: UltraFastZSet<'K>) (dict2: UltraFastZSet<'K>) =
        let result = create<'K> (max dict1.Capacity dict2.Capacity)
        
        // Add all valid entries from dict1
        for bucket in dict1.Buckets do
            if bucket.IsValid then
                insert result bucket.Key bucket.Weight
        
        // Add all valid entries from dict2  
        for bucket in dict2.Buckets do
            if bucket.IsValid then
                insert result bucket.Key bucket.Weight
        
        result
    
    /// Convert to sequence (avoiding allocations where possible)
    let toSeq (dict: UltraFastZSet<'K>) = seq {
        for bucket in dict.Buckets do
            if bucket.IsValid then
                yield (bucket.Key, bucket.Weight)
    }

/// Memory-optimized collections using struct tuples and value options
module StructOptimizedCollections =
    
    /// Value tuple for key-weight pairs to avoid heap allocation
    [<Struct>]
    type ValuePair<'K, 'V> = 
        val Key: 'K
        val Value: 'V
        
        new(key: 'K, value: 'V) = { Key = key; Value = value }
        
        member this.Deconstruct() = (this.Key, this.Value)
    
    /// Struct-based optional value to avoid Option allocation
    [<Struct>]
    type ValueOption<'T> =
        val private hasValue: bool
        val private value: 'T
        
        new(value: 'T) = { hasValue = true; value = value }
        new(hasValue: bool, value: 'T) = { hasValue = hasValue; value = value }
        
        static member None = ValueOption<'T>(false, Unchecked.defaultof<'T>)
        static member Some(value: 'T) = ValueOption<'T>(value)
        
        member this.IsSome = this.hasValue
        member this.IsNone = not this.hasValue
        member this.Value = if this.hasValue then this.value else invalidOp "No value"
        
        member this.Map(f: 'T -> 'U) =
            if this.hasValue then ValueOption<'U>.Some(f this.value)
            else ValueOption<'U>.None

/// Benchmark helpers for Phase 5.1 evaluation
module BenchmarkHelpers =
    
    /// Generate realistic DBSP workload patterns
    let generateDBSPWorkload (size: int) (insertRatio: float) (updateRatio: float) =
        let random = Random(42)
        seq {
            for _ in 1 .. size do
                let operation = random.NextDouble()
                if operation < insertRatio then
                    yield (random.Next(0, size), 1) // Insert
                elif operation < insertRatio + updateRatio then
                    yield (random.Next(0, size), random.Next(-5, 6)) // Update
                else
                    yield (random.Next(0, size), -1) // Delete
        }
    
    /// Memory usage measurement helper
    let measureMemoryUsage (f: unit -> 'T) =
        GC.Collect()
        GC.WaitForPendingFinalizers() 
        GC.Collect()
        
        let memBefore = GC.GetTotalMemory(false)
        let result = f()
        let memAfter = GC.GetTotalMemory(false)
        
        (result, memAfter - memBefore)
    
    /// Cache miss simulation helper
    let createCacheHostileData (size: int) =
        let random = Random(42)
        // Create data that will cause cache misses
        Array.init size (fun _ -> random.Next()) 
        |> Array.sortBy (fun _ -> random.Next())
        |> Array.mapi (fun i x -> (x, i))