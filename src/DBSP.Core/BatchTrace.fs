module DBSP.Core.BatchTrace

open System
open System.Runtime.CompilerServices

type Batch<'K when 'K : comparison> = {
    Pairs: ('K * int) array // sorted by key, no zero weights
}

[<MethodImpl(MethodImplOptions.AggressiveInlining)>]
let private combine (a: ('K * int) array) (b: ('K * int) array) : ('K * int) array =
    // Linear merge of two sorted arrays, summing weights; drop zeros
    let la, lb = a.Length, b.Length
    let res = Array.zeroCreate<'K * int> (la + lb)
    let mutable i, j, k = 0, 0, 0
    while i < la && j < lb do
        let ka, wa = a.[i]
        let kb, wb = b.[j]
        if ka < kb then (res.[k] <- (ka, wa); i <- i + 1; k <- k + 1)
        elif kb < ka then (res.[k] <- (kb, wb); j <- j + 1; k <- k + 1)
        else
            let w = wa + wb
            if w <> 0 then (res.[k] <- (ka, w); k <- k + 1)
            i <- i + 1; j <- j + 1
    while i < la do res.[k] <- a.[i]; i <- i + 1; k <- k + 1
    while j < lb do res.[k] <- b.[j]; j <- j + 1; k <- k + 1
    if k = res.Length then res else res.[0..k-1]

[<MethodImpl(MethodImplOptions.AggressiveInlining)>]
let private normalize (pairs: seq<'K * int>) : ('K * int) array =
    // Build, sort, and combine duplicates; drop zeros
    let arr = pairs |> Seq.toArray
    // Heuristic: for very large arrays, use bucketed sort + k-way merge to reduce comparisons
    let BUCKET_THRESHOLD = 200_000
    let useBucketed = arr.Length >= BUCKET_THRESHOLD
    if useBucketed then
        // Bucket by 12-bit hash to reduce per-bucket sorts
        let bucketsCount = 1 <<< 12
        let buckets = Array.init bucketsCount (fun _ -> System.Collections.Generic.List<'K * int>())
        for i = 0 to arr.Length - 1 do
            let (k, w) = arr.[i]
            if w <> 0 then
                let h = (k.GetHashCode() &&& 0x7FFFFFFF) % bucketsCount
                buckets.[h].Add((k, w))
        // Sort each bucket and consolidate inside the bucket
        let sortedBuckets =
            buckets
            |> Array.map (fun lst ->
                if lst.Count = 0 then [||]
                else
                    let a = lst.ToArray()
                    System.Array.Sort(a, System.Collections.Generic.Comparer<'K * int>.Create(fun (k1,_) (k2,_) -> compare k1 k2))
                    // Consolidate duplicates within bucket
                    if a.Length = 0 then a else
                        let mutable wi = 0
                        let mutable curK, curW = fst a.[0], snd a.[0]
                        for idx = 1 to a.Length - 1 do
                            let (k2, w2) = a.[idx]
                            if k2 = curK then curW <- curW + w2
                            else
                                if curW <> 0 then (a.[wi] <- (curK, curW); wi <- wi + 1)
                                curK <- k2; curW <- w2
                        if curW <> 0 then (a.[wi] <- (curK, curW); wi <- wi + 1)
                        if wi = 0 then [||] else a.[0..wi-1]
            )
        // K-way merge across buckets to produce globally sorted consolidated array
        let idx = Array.zeroCreate sortedBuckets.Length
        let len = sortedBuckets |> Array.map (fun a -> a.Length)
        let buf = System.Collections.Generic.List<'K * int>(arr.Length)
        let mutable active = true
        let inline advance i = idx.[i] <- idx.[i] + 1
        while active do
            let mutable minKey = Unchecked.defaultof<'K>
            let mutable haveMin = false
            for i = 0 to sortedBuckets.Length - 1 do
                if idx.[i] < len.[i] then
                    let (k, _) = sortedBuckets.[i].[idx.[i]]
                    if not haveMin || k < minKey then (minKey <- k; haveMin <- true)
            if not haveMin then active <- false
            else
                let kmin = minKey
                let mutable sumw = 0
                for i = 0 to sortedBuckets.Length - 1 do
                    if idx.[i] < len.[i] then
                        let (k, w) = sortedBuckets.[i].[idx.[i]]
                        if k = kmin then (sumw <- sumw + w; advance i)
                if sumw <> 0 then buf.Add((kmin, sumw))
        buf.ToArray()
    else
        Array.sortInPlaceBy fst arr
        // Combine duplicates in-place style
        if arr.Length = 0 then [||]
        else
            let mutable k = 0
            let mutable curK, curW = fst arr.[0], snd arr.[0]
            for idx = 1 to arr.Length - 1 do
                let (k2, w2) = arr.[idx]
                if k2 = curK then curW <- curW + w2
                else
                    if curW <> 0 then (arr.[k] <- (curK, curW); k <- k + 1)
                    curK <- k2; curW <- w2
            if curW <> 0 then (arr.[k] <- (curK, curW); k <- k + 1)
            if k = 0 then [||] else arr.[0..k-1]

module Batch =
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let ofSeq (pairs: seq<'K * int>) : Batch<'K> = { Pairs = normalize pairs }
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let empty<'K when 'K : comparison> : Batch<'K> = { Pairs = [||] }
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let union (a: Batch<'K>) (b: Batch<'K>) : Batch<'K> = { Pairs = combine a.Pairs b.Pairs }
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let negate (a: Batch<'K>) : Batch<'K> = { Pairs = a.Pairs |> Array.map (fun (k,w) -> (k, -w)) }
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let scalar (s: int) (a: Batch<'K>) : Batch<'K> = { Pairs = a.Pairs |> Array.choose (fun (k,w) -> let nw = s * w in if nw = 0 then None else Some (k,nw)) }
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let toSeq (a: Batch<'K>) = a.Pairs :> seq<_>
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let iter (f: 'K -> int -> unit) (a: Batch<'K>) =
        let arr = a.Pairs
        for i = 0 to arr.Length - 1 do let (k,w) = arr.[i] in f k w
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let tryFind (key: 'K) (a: Batch<'K>) =
        let arr = a.Pairs
        let mutable lo, hi = 0, arr.Length - 1
        let mutable found = None
        while lo <= hi && found.IsNone do
            let mid = (lo + hi) >>> 1
            let (k,w) = arr.[mid]
            if k = key then found <- Some w
            elif k < key then lo <- mid + 1 else hi <- mid - 1
        found

type Trace<'K when 'K : comparison> = {
    Batches: Batch<'K> list
    mutable Cached: ('K * int) array option
} with
    member this.Count =
        match this.Cached with
        | Some arr -> arr.Length
        | None -> this.Batches |> List.sumBy (fun b -> b.Pairs.Length)

module Trace =
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let empty<'K when 'K : comparison> : Trace<'K> = { Batches = []; Cached = None }

    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let ofSeq (pairs: seq<'K * int>) : Trace<'K> =
        let b = Batch.ofSeq pairs
        if b.Pairs.Length = 0 then empty else { Batches = [ b ]; Cached = None }

    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let private compact (batches: Batch<'K> list) : Batch<'K> list =
        // Simple compaction with budget: merge until <= 4 or budget exhausted
        let budgetMs =
            match System.Environment.GetEnvironmentVariable("DBSP_ZSET_COMPACT_BUDGET_MS") with
            | null -> 2
            | s -> match System.Int32.TryParse(s) with | true, v -> v | _ -> 2
        let sw = System.Diagnostics.Stopwatch.StartNew()
        let rec loop bs =
            match bs with
            | a::b::rest when List.length bs > 4 && sw.ElapsedMilliseconds < int64 budgetMs -> loop (Batch.union a b :: rest)
            | _ -> bs
        loop batches |> List.filter (fun b -> b.Pairs.Length > 0)

    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let addBatch (t: Trace<'K>) (b: Batch<'K>) : Trace<'K> =
        if b.Pairs.Length = 0 then t else { Batches = compact (b :: t.Batches); Cached = None }

    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let union (t1: Trace<'K>) (t2: Trace<'K>) : Trace<'K> =
        // Merge batch lists then compact with budget
        let merged = List.append t1.Batches t2.Batches |> compact
        { Batches = merged; Cached = None }

    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let negate (t: Trace<'K>) : Trace<'K> = { Batches = t.Batches |> List.map Batch.negate; Cached = None }

    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let scalar (s: int) (t: Trace<'K>) : Trace<'K> =
        { Batches = (t.Batches |> List.map (Batch.scalar s) |> List.filter (fun b -> b.Pairs.Length > 0)); Cached = None }

    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let toSeq (t: Trace<'K>) : seq<'K * int> =
        // Use cached consolidated array if present; otherwise materialize once and cache
        match t.Cached with
        | Some arr -> arr :> seq<_>
        | None ->
            // K-way merge across sorted batches into a buffer
            let result =
                match t.Batches with
                | [] -> [||]
                | batches ->
                    let arrs = batches |> List.map (fun b -> b.Pairs) |> List.toArray
                    let idx = Array.zeroCreate arrs.Length
                    let len = arrs |> Array.map (fun a -> a.Length)
                    let buf = System.Collections.Generic.List<'K * int>(arrs |> Array.sumBy (fun a -> a.Length))
                    let inline advance i = idx.[i] <- idx.[i] + 1
                    let mutable active = true
                    while active do
                        let mutable minKey = Unchecked.defaultof<'K>
                        let mutable haveMin = false
                        for i = 0 to arrs.Length - 1 do
                            if idx.[i] < len.[i] then
                                let (k, _) = arrs.[i].[idx.[i]]
                                if not haveMin || k < minKey then (minKey <- k; haveMin <- true)
                        if not haveMin then active <- false
                        else
                            let kmin = minKey
                            let mutable sumw = 0
                            for i = 0 to arrs.Length - 1 do
                                if idx.[i] < len.[i] then
                                    let (k, w) = arrs.[i].[idx.[i]]
                                    if k = kmin then (sumw <- sumw + w; advance i)
                            if sumw <> 0 then buf.Add((kmin, sumw))
                    buf.ToArray()
            t.Cached <- Some result
            result :> seq<_>

    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let iter (f: 'K -> int -> unit) (t: Trace<'K>) =
        for (k,w) in toSeq t do f k w
