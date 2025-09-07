/// Z-Sets: Collections with multiplicities (weights) supporting 
/// incremental computation through positive and negative weights
module DBSP.Core.ZSet

open FSharp.Data.Adaptive

/// Z-set type using high-performance HashMap for O(1) operations
/// Key -> Weight mapping where weights can be positive or negative
type ZSet<'K when 'K: comparison> = 
    { 
        /// Internal HashMap storage for O(1) operations instead of O(log N) F# Map
        Inner: HashMap<'K, int> 
    }
    
    // SRTP-compatible static members optimized for O(1) HashMap operations
    static member Zero : ZSet<'K> = { Inner = HashMap.empty }
    
    static member (+) (zset1: ZSet<'K>, zset2: ZSet<'K>) : ZSet<'K> = 
        // O(N + M) union operation - much faster than O(N * log M) Map union
        let combined = HashMap.unionWith (fun _ w1 w2 -> w1 + w2) zset1.Inner zset2.Inner
        { Inner = combined }
        
    static member (~-) (zset: ZSet<'K>) : ZSet<'K> = 
        // O(N) map operation with aggressive inlining
        { Inner = HashMap.map (fun _ weight -> -weight) zset.Inner }
        
    static member (*) (scalar: int, zset: ZSet<'K>) : ZSet<'K> = 
        { Inner = HashMap.map (fun _ weight -> scalar * weight) zset.Inner }

    /// Check if the ZSet is empty (contains no elements with non-zero weight)
    member this.IsEmpty = 
        HashMap.isEmpty this.Inner || 
        HashMap.forall (fun _ weight -> weight = 0) this.Inner

    /// Get the weight of a specific key
    member this.GetWeight(key: 'K) =
        HashMap.tryFind key this.Inner |> Option.defaultValue 0

    /// Get all keys with non-zero weights
    member this.Keys =
        HashMap.toSeq this.Inner
        |> Seq.filter (fun (_, weight) -> weight <> 0)
        |> Seq.map fst

    /// Get total number of elements (sum of absolute weights)
    member this.Count =
        HashMap.fold (fun acc _ weight -> acc + abs weight) 0 this.Inner

/// Module functions for ZSet operations using F# 7+ simplified SRTP syntax
module ZSet =

    /// Create empty ZSet
    let empty<'K when 'K: comparison> : ZSet<'K> = { Inner = HashMap.empty }

    /// Create singleton ZSet with given key and weight
    let singleton key weight = 
        { Inner = HashMap.ofList [(key, weight)] }

    /// Create ZSet from sequence of key-weight pairs
    let ofSeq (pairs: seq<'K * int>) =
        let hashMap = HashMap.ofSeq pairs
        { Inner = hashMap }

    /// Create ZSet from list of key-weight pairs  
    let ofList (pairs: ('K * int) list) =
        { Inner = HashMap.ofList pairs }

    /// Convert ZSet to sequence of key-weight pairs (excluding zero weights)
    let toSeq (zset: ZSet<'K>) =
        HashMap.toSeq zset.Inner
        |> Seq.filter (fun (_, weight) -> weight <> 0)

    /// Convenient inline functions using F# 7+ simplified SRTP syntax
    let inline add (zset1: ZSet<'K>) (zset2: ZSet<'K>) = zset1 + zset2
    let inline negate (zset: ZSet<'K>) = -zset
    let inline scalarMultiply scalar (zset: ZSet<'K>) = scalar * zset

    /// Insert a key with given weight (adds to existing weight)
    let insert key weight (zset: ZSet<'K>) =
        let newWeight = zset.GetWeight(key) + weight
        if newWeight = 0 then
            // Remove key if weight becomes zero
            { Inner = HashMap.remove key zset.Inner }
        else
            { Inner = HashMap.add key newWeight zset.Inner }

    /// Remove a key by adding negative weight
    let remove key weight zset =
        insert key (-weight) zset

    /// Union of two ZSets (same as addition)
    let union zset1 zset2 = add zset1 zset2

    /// Difference of two ZSets
    let difference zset1 zset2 = add zset1 (negate zset2)

    /// Filter ZSet by key predicate
    let filter predicate zset =
        { Inner = HashMap.filter (fun key _ -> predicate key) zset.Inner }

    /// Map over keys (preserving weights)
    let mapKeys f zset =
        HashMap.toSeq zset.Inner
        |> Seq.map (fun (key, weight) -> (f key, weight))
        |> ofSeq

    /// Fold over ZSet entries
    let fold folder state zset =
        HashMap.fold (fun acc key weight -> 
            if weight <> 0 then folder acc key weight else acc
        ) state zset.Inner