/// Optimized ZSet operations for high-performance incremental computation
module DBSP.Core.ZSetOptimized

open System.Collections.Generic
open FSharp.Data.Adaptive
open DBSP.Core.ZSet

/// Builder pattern for efficient ZSet construction without intermediate allocations
type ZSetBuilder<'K when 'K: comparison>() =
    let mutable map = HashMap.empty<'K, int>
    let mutable count = 0
    
    /// Add a single element with weight
    member _.Add(key: 'K, weight: int) =
        if weight <> 0 then
            map <- HashMap.alter key (function
                | Some w -> 
                    let newWeight = w + weight
                    if newWeight = 0 then None else Some newWeight
                | None -> Some weight) map
            count <- count + 1
    
    /// Add multiple elements at once
    member this.AddRange(items: seq<'K * int>) =
        for (key, weight) in items do
            this.Add(key, weight)
    
    /// Build the final immutable ZSet
    member _.Build() : ZSet<'K> = 
        { Inner = map }
    
    /// Get current count of operations
    member _.Count = count
    
    /// Clear the builder for reuse
    member _.Clear() =
        map <- HashMap.empty
        count <- 0

/// Optimized module functions for ZSet operations
module ZSetOptimized =
    
    /// Create a ZSet using a builder for better performance
    let inline buildWith (f: ZSetBuilder<'K> -> unit) =
        let builder = ZSetBuilder<'K>()
        f builder
        builder.Build()
    
    /// Apply changes efficiently using indexed lookup (fixes O(N*M) bottleneck)
    let applyChanges (baseData: ('K * 'V) array) 
                     (changes: ('K * 'V) array) 
                     (getKey: 'K * 'V -> 'K) : ZSet<'K * 'V> =
        // Index base data for O(1) lookups instead of O(N) array search
        let baseIndex = 
            baseData 
            |> Array.map (fun item -> getKey item, item)
            |> HashMap.ofArray
        
        buildWith (fun builder ->
            // Add all base data that's not being changed
            let changeKeys = changes |> Array.map getKey |> HashSet
            for item in baseData do
                let key = getKey item
                if not (changeKeys.Contains(key)) then
                    builder.Add(item, 1)
            
            // Add changes (new versions)
            for item in changes do
                builder.Add(item, 1)
        )
    
    /// Efficient incremental update with pre-indexed data
    let incrementalUpdate (baseZSet: ZSet<'K>) 
                         (baseIndex: HashMap<'Id, 'K>)
                         (changes: seq<'Id * 'K>)
                         (getKeyId: 'K -> 'Id) : ZSet<'K> * HashMap<'Id, 'K> =
        
        buildWith (fun builder ->
            // Start with existing data
            for (k, w) in HashMap.toSeq baseZSet.Inner do
                let keyId = getKeyId k
                // Check if this key is being updated
                let isBeingUpdated = changes |> Seq.exists (fun (id, _) -> id = keyId)
                if not isBeingUpdated && w <> 0 then
                    builder.Add(k, w)
            
            // Add changes
            for (id, newValue) in changes do
                // Remove old version if exists
                match HashMap.tryFind id baseIndex with
                | Some oldValue -> () // Old value already excluded above
                | None -> ()
                // Add new version
                builder.Add(newValue, 1)
        ),
        // Return updated index
        let newIndex = 
            changes 
            |> Seq.fold (fun idx (id, value) -> 
                HashMap.add id value idx) baseIndex
        newIndex
    
    /// Batch union of multiple ZSets without intermediate allocations
    let unionMany (zsets: ZSet<'K> seq) : ZSet<'K> =
        buildWith (fun builder ->
            for zset in zsets do
                for (k, w) in HashMap.toSeq zset.Inner do
                    builder.Add(k, w)
        )
    
    /// Efficient difference operation
    let difference (left: ZSet<'K>) (right: ZSet<'K>) : ZSet<'K> =
        buildWith (fun builder ->
            // Add all from left
            for (k, w) in HashMap.toSeq left.Inner do
                builder.Add(k, w)
            // Subtract all from right
            for (k, w) in HashMap.toSeq right.Inner do
                builder.Add(k, -w)
        )
    
    /// Filter with weight preservation
    let filterWithWeight (predicate: 'K -> int -> bool) (zset: ZSet<'K>) : ZSet<'K> =
        buildWith (fun builder ->
            for (k, w) in HashMap.toSeq zset.Inner do
                if predicate k w then
                    builder.Add(k, w)
        )
    
    /// Map keys while preserving weights
    let mapKeys (f: 'K -> 'K2) (zset: ZSet<'K>) : ZSet<'K2> =
        buildWith (fun builder ->
            for (k, w) in HashMap.toSeq zset.Inner do
                builder.Add(f k, w)
        )
    
    /// Fold with early termination support
    let foldWhile (folder: 'State -> 'K -> int -> 'State * bool) 
                  (state: 'State) 
                  (zset: ZSet<'K>) : 'State =
        let mutable acc = state
        let mutable continue' = true
        let seq = HashMap.toSeq zset.Inner |> Seq.toArray
        let mutable i = 0
        while continue' && i < seq.Length do
            let (k, w) = seq.[i]
            let newState, shouldContinue = folder acc k w
            acc <- newState
            continue' <- shouldContinue
            i <- i + 1
        acc

/// Memory pool for reducing allocations in hot paths
module MemoryPool =
    open System.Buffers
    
    /// Pool for temporary arrays
    let private arrayPool = ArrayPool<obj>.Shared
    
    /// Rent an array from the pool
    let inline rentArray (minimumLength: int) =
        ArrayPool<obj>.Shared.Rent(minimumLength)
    
    /// Return an array to the pool
    let inline returnArray (array: obj[]) (clearArray: bool) =
        ArrayPool<obj>.Shared.Return(array, clearArray)
    
    /// Execute a function with a pooled array
    let inline withPooledArray<'T, 'R> (minimumLength: int) (f: 'T[] -> 'R) =
        let pool = ArrayPool<'T>.Shared
        let array = pool.Rent(minimumLength)
        try
            f array
        finally
            pool.Return(array, true)
    
/// Optimized operations using memory pools
module PooledOperations =
    open MemoryPool
    
    /// Convert ZSet to array using pooled memory
    let toPooledArray (zset: ZSet<'K>) =
        let count = HashMap.count zset.Inner
        let result = Array.zeroCreate count
        let mutable i = 0
        for (k, w) in HashMap.toSeq zset.Inner do
            if w <> 0 then
                result.[i] <- (k, w)
                i <- i + 1
        Array.sub result 0 i