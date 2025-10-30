namespace DBSP.Core

open System
open System.Collections.Generic
open DBSP.Core.ZSet

/// <summary>
/// Represents a builder for constructing delta <see cref="ZSet{TKey}"/> values using intent-centric operations.
/// </summary>
type ZSetDeltaBuilder<'K when 'K : comparison>() =
    let mutable builder = ZSet.ZSetBuilder<'K>()

    /// <summary>Adds an insertion for <paramref name="key"/> (weight +1).</summary>
    member this.AddInsert(key: 'K) =
        builder.Add(key, 1)
        this

    /// <summary>Adds a deletion for <paramref name="key"/> (weight -1).</summary>
    member this.AddDelete(key: 'K) =
        builder.Add(key, -1)
        this

    /// <summary>Adds a move operation from <paramref name="oldKey"/> to <paramref name="newKey"/>.</summary>
    member this.AddMove(oldKey: 'K, newKey: 'K) =
        if oldKey <> newKey then
            builder.Add(oldKey, -1)
            builder.Add(newKey, 1)
        this

    /// <summary>Adds an explicit weight for <paramref name="key"/>.</summary>
    member this.AddWeight(key: 'K, weight: int) =
        if weight <> 0 then
            builder.Add(key, weight)
        this

    /// <summary>Adds multiple insertions.</summary>
    member this.AddInserts(keys: IEnumerable<'K>) =
        for key in keys do
            builder.Add(key, 1)
        this

    /// <summary>Adds multiple deletions.</summary>
    member this.AddDeletes(keys: IEnumerable<'K>) =
        for key in keys do
            builder.Add(key, -1)
        this

    /// <summary>Adds weighted entries.</summary>
    member this.AddWeights(weights: IEnumerable<ValueTuple<'K, int>>) =
        for struct (key, weight) in weights do
            if weight <> 0 then
                builder.Add(key, weight)
        this

    /// <summary>Clears the current accumulator so the builder can be reused.</summary>
    member this.Clear() =
        builder <- ZSet.ZSetBuilder<'K>()
        this

    /// <summary>Builds the <see cref="ZSet{TKey}"/> representing the accumulated delta.</summary>
    member this.ToZSet() =
        builder.Build()

    /// <summary>Creates a new <see cref="ZSetDeltaBuilder{TKey}"/> instance.</summary>
    static member Create() =
        ZSetDeltaBuilder<'K>()

/// <summary>Helper methods for creating delta <see cref="ZSet{TKey}"/> instances using common patterns.</summary>
[<AbstractClass; Sealed>]
type ZSetDelta =
    /// <summary>Creates a new <see cref="ZSetDeltaBuilder{TKey}"/>.</summary>
    static member Create<'K when 'K : comparison>() =
        ZSetDeltaBuilder<'K>()

    /// <summary>Constructs a delta containing insertions for the provided <paramref name="keys"/>.</summary>
    static member OfInserts<'K when 'K : comparison>(keys: IEnumerable<'K>) =
        ZSetDeltaBuilder<'K>()
            .AddInserts(keys)
            .ToZSet()

    /// <summary>Constructs a delta containing deletions for the provided <paramref name="keys"/>.</summary>
    static member OfDeletes<'K when 'K : comparison>(keys: IEnumerable<'K>) =
        ZSetDeltaBuilder<'K>()
            .AddDeletes(keys)
            .ToZSet()

    /// <summary>Constructs a delta representing move operations.</summary>
    static member OfMoves<'K when 'K : comparison>(moves: IEnumerable<ValueTuple<'K, 'K>>) =
        let builder = ZSetDeltaBuilder<'K>()
        for struct (oldKey, newKey) in moves do
            builder.AddMove(oldKey, newKey) |> ignore
        builder.ToZSet()

    /// <summary>Constructs a delta from explicit weights.</summary>
    static member OfWeights<'K when 'K : comparison>(weights: IEnumerable<ValueTuple<'K, int>>) =
        ZSetDeltaBuilder<'K>()
            .AddWeights(weights)
            .ToZSet()

/// <summary>F#-centric helper functions for building delta ZSets.</summary>
[<AutoOpen>]
module ZSetDeltaFunctions =
    /// <summary>Creates a new builder for the given key type.</summary>
    let inline delta<'K when 'K : comparison>() =
        ZSetDeltaBuilder<'K>()

    /// <summary>Consumes a sequence of change helpers and returns the aggregated delta.</summary>
    let inline deltaOfSeq<'K when 'K : comparison> (changes: seq<'K * int>) =
        let builder = ZSetDeltaBuilder<'K>()
        builder.AddWeights(changes |> Seq.map (fun (k, w) -> ValueTuple<'K, int>(k, w))) |> ignore
        builder.ToZSet()
