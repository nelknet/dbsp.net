/// Z-Sets: Collections with multiplicities (weights) supporting
/// incremental computation through positive and negative weights
module DBSP.Core.ZSet

open System.Runtime.CompilerServices
open DBSP.Core.BatchTrace
open FSharp.Data.Adaptive
open System

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
            mutable Occupied: System.Collections.Generic.List<int>
            mutable Tombstones: int
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
            { Buckets = Array.create actualCapacity ZSetBucket.Empty
              Count = 0
              Mask = actualCapacity - 1
              Occupied = new System.Collections.Generic.List<int>(actualCapacity)
              Tombstones = 0
              Comparer = comparer }

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
                    dict.Tombstones <- dict.Tombstones + 1
                    // Adaptive compaction / shrinking after deletions
                    let cap = dict.Buckets.Length
                    // If too many tombstones, compact in place (threshold ~25%)
                    if dict.Tombstones * 4 > cap then
                        let old = dict.Buckets
                        let cap2 = dict.Buckets.Length
                        dict.Buckets <- Array.create cap2 ZSetBucket.Empty
                        dict.Mask <- cap2 - 1
                        dict.Count <- 0
                        dict.Occupied.Clear()
                        dict.Tombstones <- 0
                        for b in old do if b.IsValid then insertOrUpdate dict b.Key b.Weight
                    else
                        // If very sparse, shrink to a tighter capacity
                        let targetCap =
                            if dict.Count * 4 < cap && cap > 16 then
                                // shrink to next pow2 above count*2 (load ~0.5)
                                let desired = max 16 (dict.Count * 2)
                                1 <<< (32 - System.Numerics.BitOperations.LeadingZeroCount(uint32 (desired - 1)))
                            else cap
                        if targetCap < cap then
                            let old = dict.Buckets
                            dict.Buckets <- Array.create targetCap ZSetBucket.Empty
                            dict.Mask <- targetCap - 1
                            dict.Count <- 0
                            dict.Occupied.Clear()
                            dict.Tombstones <- 0
                            for b in old do if b.IsValid then insertOrUpdate dict b.Key b.Weight
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
                    dict.Occupied.Clear()
                    dict.Tombstones <- 0
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
                        dict.Occupied.Add(idx)
                        inserted <- true
                    elif b.IsValid && b.HashCode = h && dict.Comparer.Equals(b.Key, k) then
                        b.Weight <- b.Weight + w
                        if b.Weight = 0 then
                            b.HashCode <- Helpers.HashCode.tombstone
                            dict.Count <- dict.Count - 1
                            dict.Tombstones <- dict.Tombstones + 1
                        inserted <- true
                    elif b.Distance < d then
                        let tmpH, tmpD, tmpK, tmpW = b.HashCode, b.Distance, b.Key, b.Weight
                        b.HashCode <- h; b.Distance <- d; b.Key <- k; b.Weight <- w
                        h <- tmpH; d <- tmpD; k <- tmpK; w <- tmpW
                        idx <- (idx + 1) &&& dict.Mask; d <- d + 1uy
                    else
                        idx <- (idx + 1) &&& dict.Mask; d <- d + 1uy

        /// Rehash and compact (remove tombstones and rebuild occupied list)
        let compact (dict: FastZSet<'K>) =
            let old = dict.Buckets
            let cap = dict.Buckets.Length
            dict.Buckets <- Array.create cap ZSetBucket.Empty
            dict.Mask <- cap - 1
            dict.Count <- 0
            dict.Occupied.Clear()
            dict.Tombstones <- 0
            for b in old do if b.IsValid then insertOrUpdate dict b.Key b.Weight

        let ofSeq (pairs: seq<'K * int>) =
            // If we can pre-count, pre-size for fewer resizes
            let coll = match pairs with :? System.Collections.Generic.ICollection<_> as c -> Some c.Count | _ -> None
            let initialCap = match coll with Some n when n > 0 -> max 16 (1 <<< (32 - System.Numerics.BitOperations.LeadingZeroCount(uint32 (n * 2 - 1)))) | _ -> 16
            let dict = empty<'K> initialCap
            for (key, weight) in pairs do if weight <> 0 then insertOrUpdate dict key weight
            dict

        [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
        let tryGetWeight (dict: FastZSet<'K>) (key: 'K) =
            let hash = dict.Comparer.GetHashCode(key) &&& Helpers.POSITIVE_INT_MASK
            let bucketIndex = findBucket dict key hash
            if bucketIndex >= 0 then dict.Buckets.[bucketIndex].Weight else 0

        [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
        let tryFind (dict: FastZSet<'K>) (key: 'K) =
            let hash = dict.Comparer.GetHashCode(key) &&& Helpers.POSITIVE_INT_MASK
            let bucketIndex = findBucket dict key hash
            if bucketIndex >= 0 then Some dict.Buckets.[bucketIndex].Weight else None

        [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
        let containsKey (dict: FastZSet<'K>) (key: 'K) =
            let hash = dict.Comparer.GetHashCode(key) &&& Helpers.POSITIVE_INT_MASK
            let bucketIndex = findBucket dict key hash
            bucketIndex >= 0 && dict.Buckets.[bucketIndex].IsValid

        [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
        let ofList (pairs: ('K * int) list) = ofSeq pairs

        let union (dict1: FastZSet<'K>) (dict2: FastZSet<'K>) =
            // Pre-size using counts to reduce resizes and iteration cost
            let estimated = dict1.Count + dict2.Count
            let initialCap = max 16 (1 <<< (32 - System.Numerics.BitOperations.LeadingZeroCount(uint32 (max 1 (estimated * 2 - 1)))))
            let result = empty<'K> initialCap
            // Iterate only occupied buckets using the occupied index list
            for i = 0 to dict1.Occupied.Count - 1 do
                let bi = dict1.Occupied.[i]
                let b = dict1.Buckets.[bi]
                if b.IsValid then insertOrUpdate result b.Key b.Weight
            for i = 0 to dict2.Occupied.Count - 1 do
                let bi = dict2.Occupied.[i]
                let b = dict2.Buckets.[bi]
                if b.IsValid then insertOrUpdate result b.Key b.Weight
            result

        let toSeq (dict: FastZSet<'K>) =
            // Iterate over occupied indices only (O(count))
            seq {
                for i = 0 to dict.Occupied.Count - 1 do
                    let bi = dict.Occupied.[i]
                    let b = dict.Buckets.[bi]
                    if b.IsValid then yield (b.Key, b.Weight)
            }

        // Struct enumerator to avoid iterator allocations in hot loops
        [<Struct>]
        type Enumerator<'K when 'K : equality> =
            val mutable private Snapshot : int[]
            val mutable private Index : int
            val mutable private Dict : FastZSet<'K>
            val mutable private Curr : 'K * int
            new(dict: FastZSet<'K>) = { Snapshot = dict.Occupied.ToArray(); Index = -1; Dict = dict; Curr = Unchecked.defaultof<_> }
            member this.MoveNext() =
                let mutable i = this.Index
                let mutable found = false
                while (not found) && (i + 1 < this.Snapshot.Length) do
                    i <- i + 1
                    let bi = this.Snapshot.[i]
                    let b = this.Dict.Buckets.[bi]
                    if b.IsValid then
                        this.Curr <- (b.Key, b.Weight)
                        found <- true
                this.Index <- i
                found
            member this.Current = this.Curr

        let inline iter (f: 'K -> int -> unit) (dict: FastZSet<'K>) =
            let mutable e = Enumerator<'K>(dict)
            let mutable cont = e.MoveNext()
            while cont do
                let (k,w) = e.Current
                f k w
                cont <- e.MoveNext()

module FZ = Collections.FastZSet

// Global adaptive policy and arranged-subscriber hinting
module private Policy =
    let private envInt name def =
        match System.Environment.GetEnvironmentVariable(name) with
        | null -> def
        | s -> match System.Int32.TryParse(s) with | true, v -> v | _ -> def
    let DEFAULT_S = envInt "DBSP_ZSET_FLUSH_SIZE" 65536
    let DEFAULT_T_MS = envInt "DBSP_ZSET_FLUSH_TIME_MS" 20
    let DEFAULT_R = envInt "DBSP_ZSET_LEVEL_FANOUT" 4
    let DEFAULT_N = envInt "DBSP_ZSET_SMALLSET_N" 512
    let DEFAULT_COMPACT_BUDGET_MS = envInt "DBSP_ZSET_COMPACT_BUDGET_MS" 2
    let mutable arrangedSubscribers = 0
    let arrangedSubscribersCount () = arrangedSubscribers
    let incArranged() = System.Threading.Interlocked.Increment(&arrangedSubscribers) |> ignore
    let decArranged() = System.Threading.Interlocked.Decrement(&arrangedSubscribers) |> ignore

/// Z-set type backed by FastZSet storage (equality-constrained keys)
type ZSetBackend =
    | Batch
    | Fast
    | Hash
    | Adaptive

[<CustomEquality; NoComparison>]
type ZSet<'K when 'K : comparison> =
    {
        Backend: ZSetBackend
        // When Backend=Adaptive, Fast is the memtable overlay and Batch is the Trace spine
        Fast: FZ.FastZSet<'K>
        Batch: Trace<'K>
        Hash: HashMap<'K, int>
        // Adaptive-only small-vector storage for tiny sets; empty when not used
        Small: ('K * int) array
        // Adaptive flush bookkeeping (UTC ticks of last flush)
        LastFlushTicks: int64
    }
with
    override this.Equals(obj: obj) =
        match obj with
        | :? ZSet<'K> as other ->
            // Logical equality: same consolidated key/weight multiset
            let logicalSeq (z: ZSet<'K>) : seq<'K * int> =
                match z.Backend with
                | ZSetBackend.Batch -> Trace.toSeq z.Batch
                | ZSetBackend.Fast -> FZ.toSeq z.Fast
                | ZSetBackend.Hash -> HashMap.toSeq z.Hash |> Seq.filter (fun (_,w) -> w <> 0)
                | ZSetBackend.Adaptive ->
                    let baseTrace = z.Batch
                    let t1 = if z.Small.Length > 0 then BatchTrace.Trace.addBatch baseTrace (BatchTrace.Batch.ofSeq z.Small) else baseTrace
                    let t2 = if z.Fast.Count > 0 then BatchTrace.Trace.addBatch t1 (BatchTrace.Batch.ofSeq (FZ.toSeq z.Fast)) else t1
                    BatchTrace.Trace.toSeq t2
            let a = (logicalSeq this |> Seq.toArray)
            let b = (logicalSeq other |> Seq.toArray)
            System.Array.Sort(a)
            System.Array.Sort(b)
            a = b
        | _ -> false
    override this.GetHashCode() =
        // Hash over a few consolidated entries for stability
        let mutable hc = 17
        let mutable i = 0
        // use same logical consolidated sequence as Equals
        let logicalSeq (z: ZSet<'K>) : seq<'K * int> =
            match z.Backend with
            | ZSetBackend.Batch -> Trace.toSeq z.Batch
            | ZSetBackend.Fast -> FZ.toSeq z.Fast
            | ZSetBackend.Hash -> HashMap.toSeq z.Hash |> Seq.filter (fun (_,w) -> w <> 0)
            | ZSetBackend.Adaptive ->
                let baseTrace = z.Batch
                let t1 = if z.Small.Length > 0 then BatchTrace.Trace.addBatch baseTrace (BatchTrace.Batch.ofSeq z.Small) else baseTrace
                let t2 = if z.Fast.Count > 0 then BatchTrace.Trace.addBatch t1 (BatchTrace.Batch.ofSeq (FZ.toSeq z.Fast)) else t1
                BatchTrace.Trace.toSeq t2
        for (k,w) in (logicalSeq this) do
            // sample first ~8 pairs to bound cost
            if i < 8 then
                hc <- System.HashCode.Combine(hc, k.GetHashCode(), w.GetHashCode())
                i <- i + 1
        hc

// Algebraic operations
// Internal: flush Adaptive overlays into trace with S/T policies (available to members below)
let private flushAdaptiveInternal (z: ZSet<'K>) : ZSet<'K> =
    if z.Backend <> ZSetBackend.Adaptive then z else
    let now = System.DateTime.UtcNow.Ticks
    let elapsedMs = if z.LastFlushTicks = 0L then System.Int64.MaxValue else int ((now - z.LastFlushTicks) / System.TimeSpan.TicksPerMillisecond)
    // Subscriber-aware thresholds: tighten when arranged subscribers exist
    let subs = Policy.arrangedSubscribersCount()
    let tMs = if subs > 0 then max 1 (Policy.DEFAULT_T_MS / 4) else Policy.DEFAULT_T_MS
    let sTh = if subs > 0 then max 1024 (Policy.DEFAULT_S / 4) else Policy.DEFAULT_S
    let shouldTimeFlush = elapsedMs >= tMs
    let shouldSizeFlush = (z.Fast.Count + z.Small.Length) >= sTh
    if (z.Fast.Count = 0 && z.Small.Length = 0) || (not shouldTimeFlush && not shouldSizeFlush) then z
    else
        let mutable trace = z.Batch
        if z.Small.Length > 0 then
            let bs = BatchTrace.Batch.ofSeq z.Small
            trace <- BatchTrace.Trace.addBatch trace bs
        if z.Fast.Count > 0 then
            let bs = z.Fast |> FZ.toSeq |> BatchTrace.Batch.ofSeq
            trace <- BatchTrace.Trace.addBatch trace bs
        // compactIfNeeded logic (R=4)
        let compactIfNeeded (t: Trace<'K>) : Trace<'K> =
            if List.length t.Batches > 4 then
                let mutable batches = t.Batches
                while List.length batches > 4 do
                    match batches with
                    | a::b::rest -> batches <- (BatchTrace.Batch.union a b) :: rest
                    | _ -> ()
                { Batches = batches |> List.filter (fun b -> b.Pairs.Length > 0); Cached = None }
            else t
        let trace = compactIfNeeded trace
        { z with Fast = FZ.empty 0; Small = [||]; Batch = trace; LastFlushTicks = now }

// Internal: normalize Adaptive representation into a consolidated Trace (deterministic logical view)
let private normalizeAdaptiveInternal (z: ZSet<'K>) : ZSet<'K> =
    if z.Backend <> ZSetBackend.Adaptive then z else
    let mutable trace = z.Batch
    if z.Small.Length > 0 then
        trace <- BatchTrace.Trace.addBatch trace (BatchTrace.Batch.ofSeq z.Small)
    if z.Fast.Count > 0 then
        trace <- BatchTrace.Trace.addBatch trace (BatchTrace.Batch.ofSeq (FZ.toSeq z.Fast))
    // Compact deterministically to a small number of batches
    let compactIfNeeded (t: Trace<'K>) : Trace<'K> =
        if List.length t.Batches > 4 then
            let mutable batches = t.Batches
            while List.length batches > 4 do
                match batches with
                | a::b::rest -> batches <- (BatchTrace.Batch.union a b) :: rest
                | _ -> ()
            { Batches = batches |> List.filter (fun b -> b.Pairs.Length > 0); Cached = None }
        else t
    let trace = compactIfNeeded trace
    { z with Fast = FZ.empty 0; Small = [||]; Batch = trace }

type ZSet<'K when 'K : comparison> with
    static member Zero : ZSet<'K> = { Backend = ZSetBackend.Batch; Fast = FZ.empty 0; Batch = Trace.empty<'K>; Hash = HashMap.empty; Small = [||]; LastFlushTicks = 0L }

    static member (+) (a: ZSet<'K>, b: ZSet<'K>) : ZSet<'K> =
        // If backends differ, fall back to generic logical addition via consolidated sequences
        if a.Backend <> b.Backend then
            let seqOf (z: ZSet<'K>) =
                match z.Backend with
                | ZSetBackend.Batch -> Trace.toSeq z.Batch
                | ZSetBackend.Fast -> FZ.toSeq z.Fast
                | ZSetBackend.Hash -> HashMap.toSeq z.Hash |> Seq.filter (fun (_,w) -> w <> 0)
                | ZSetBackend.Adaptive -> let z' = normalizeAdaptiveInternal z in Trace.toSeq z'.Batch
            let combined = Seq.append (seqOf a) (seqOf b)
            let t = Trace.ofSeq combined
            { Backend = ZSetBackend.Adaptive; Fast = FZ.empty 0; Batch = t; Hash = HashMap.empty; Small = [||]; LastFlushTicks = 0L }
        else
        match a.Backend with
        | ZSetBackend.Batch -> { a with Batch = Trace.union a.Batch b.Batch }
        | ZSetBackend.Fast -> { a with Fast = FZ.union a.Fast b.Fast }
        | ZSetBackend.Hash ->
            let merged = HashMap.unionWith (fun _ w1 w2 -> let s = w1 + w2 in if s = 0 then 0 else s) a.Hash b.Hash
            { a with Hash = merged }
        | ZSetBackend.Adaptive ->
            // Normalize inputs to avoid overlay double-counting before union
            let aN = normalizeAdaptiveInternal a
            let bN = normalizeAdaptiveInternal b
            // Adaptive union: merge memtables and append traces; keep small vectors in memtable path
            let resultMem =
                let cap = aN.Fast.Count + bN.Fast.Count + aN.Small.Length + bN.Small.Length
                let mem = FZ.empty<'K> (max 16 cap)
                // bring over a
                for (k,w) in aN.Small do if w <> 0 then FZ.insertOrUpdate mem k w
                FZ.iter (fun k w -> if w <> 0 then FZ.insertOrUpdate mem k w) aN.Fast
                // bring over b
                for (k,w) in bN.Small do if w <> 0 then FZ.insertOrUpdate mem k w
                FZ.iter (fun k w -> if w <> 0 then FZ.insertOrUpdate mem k w) bN.Fast
                mem
            let resultTrace = Trace.union aN.Batch bN.Batch
            { aN with Fast = resultMem; Batch = resultTrace; Small = [||] } |> normalizeAdaptiveInternal

    static member (~-) (z: ZSet<'K>) : ZSet<'K> =
        match z.Backend with
        | ZSetBackend.Batch -> { z with Batch = Trace.negate z.Batch }
        | ZSetBackend.Fast ->
            let res = FZ.empty<'K> z.Fast.Capacity
            for (k,w) in FZ.toSeq z.Fast do if w <> 0 then FZ.insertOrUpdate res k (-w)
            { z with Fast = res }
        | ZSetBackend.Hash -> { z with Hash = HashMap.map (fun _ w -> -w) z.Hash }
        | ZSetBackend.Adaptive ->
            // Negate memtable, small, and batches
            let mem =
                let m2 = FZ.empty<'K> (max 16 z.Fast.Count)
                FZ.iter (fun k w -> if w <> 0 then FZ.insertOrUpdate m2 k (-w)) z.Fast
                m2
            let small = z.Small |> Array.map (fun (k,w) -> (k, -w))
            { z with Fast = mem; Batch = Trace.negate z.Batch; Small = small } |> normalizeAdaptiveInternal

    static member (*) (s: int, z: ZSet<'K>) : ZSet<'K> =
        if s = 1 then z
        else
            match z.Backend with
            | ZSetBackend.Batch -> { z with Batch = Trace.scalar s z.Batch }
            | ZSetBackend.Fast ->
                if s = 0 then { z with Fast = FZ.empty 0 } else
                let res = FZ.empty<'K> z.Fast.Capacity
                for (k,w) in FZ.toSeq z.Fast do let nw = s * w in if nw <> 0 then FZ.insertOrUpdate res k nw
                { z with Fast = res }
            | ZSetBackend.Hash -> if s = 0 then { z with Hash = HashMap.empty } else { z with Hash = HashMap.map (fun _ w -> s * w) z.Hash }
            | ZSetBackend.Adaptive ->
                if s = 0 then { z with Fast = FZ.empty 0; Batch = Trace.empty<'K>; Small = [||] } |> normalizeAdaptiveInternal
                else
                    let mem =
                        let m2 = FZ.empty<'K> (max 16 z.Fast.Count)
                        FZ.iter (fun k w -> let nw = s * w in if nw <> 0 then FZ.insertOrUpdate m2 k nw) z.Fast
                        m2
                    let small = z.Small |> Array.choose (fun (k,w) -> let nw = s * w in if nw = 0 then None else Some (k,nw))
                    { z with Fast = mem; Batch = Trace.scalar s z.Batch; Small = small } |> normalizeAdaptiveInternal

    member this.IsEmpty =
        match this.Backend with
        | ZSetBackend.Batch -> List.isEmpty this.Batch.Batches
        | ZSetBackend.Fast -> this.Fast.Count = 0
        | ZSetBackend.Hash -> HashMap.isEmpty this.Hash
        | ZSetBackend.Adaptive -> (this.Small.Length = 0) && (this.Fast.Count = 0) && (List.isEmpty this.Batch.Batches)

    member this.GetWeight(key: 'K) =
        match this.Backend with
        | ZSetBackend.Batch -> Trace.toSeq this.Batch |> Seq.tryPick (fun (k,w) -> if k = key then Some w else None) |> Option.defaultValue 0
        | ZSetBackend.Fast -> FZ.tryGetWeight this.Fast key
        | ZSetBackend.Hash -> HashMap.tryFind key this.Hash |> Option.defaultValue 0
        | ZSetBackend.Adaptive ->
            // Probe small vector, then memtable, then batches; sum weights
            let mutable acc = 0
            // small vector binary search
            if this.Small.Length > 0 then
                let arr = this.Small
                let mutable lo, hi = 0, arr.Length - 1
                let mutable found = false
                while lo <= hi && not found do
                    let mid = (lo + hi) >>> 1
                    let (k,w) = arr.[mid]
                    if k = key then (acc <- acc + w; found <- true)
                    elif k < key then lo <- mid + 1 else hi <- mid - 1
            // memtable
            acc <- acc + (FZ.tryGetWeight this.Fast key)
            // batches (sum over batches)
            for b in this.Batch.Batches do
                match BatchTrace.Batch.tryFind key b with
                | Some w -> acc <- acc + w
                | None -> ()
            acc

    member this.Keys =
        match this.Backend with
        | ZSetBackend.Batch -> Trace.toSeq this.Batch |> Seq.map fst
        | ZSetBackend.Fast -> FZ.toSeq this.Fast |> Seq.map fst
        | ZSetBackend.Hash -> HashMap.toSeq this.Hash |> Seq.filter (fun (_,w) -> w <> 0) |> Seq.map fst
        | ZSetBackend.Adaptive ->
            // Ensure a consistent, consolidated view by flushing before enumeration
            let z = flushAdaptiveInternal this
            Trace.toSeq z.Batch |> Seq.map fst

    member this.Count =
        match this.Backend with
        | ZSetBackend.Batch -> Trace.toSeq this.Batch |> Seq.fold (fun acc (_,w) -> acc + abs w) 0
        | ZSetBackend.Fast -> FZ.toSeq this.Fast |> Seq.fold (fun acc (_,w) -> acc + abs w) 0
        | ZSetBackend.Hash -> HashMap.fold (fun acc _ w -> acc + abs w) 0 this.Hash
        | ZSetBackend.Adaptive ->
            // Flush before computing to avoid double counting overlays
            let z = flushAdaptiveInternal this
            Trace.toSeq z.Batch |> Seq.fold (fun acc (_,w) -> acc + abs w) 0

/// Module functions for ZSet operations
module ZSet =

    // Adaptive policy defaults (can be tuned)
    // Defaults can be overridden via env variables (read once)
    let internal DEFAULT_S = Policy.DEFAULT_S
    let internal DEFAULT_T_MS = Policy.DEFAULT_T_MS
    let internal DEFAULT_R = Policy.DEFAULT_R
    let internal DEFAULT_N = Policy.DEFAULT_N
    let internal DEFAULT_COMPACT_BUDGET_MS = Policy.DEFAULT_COMPACT_BUDGET_MS
    let internal arrangedSubscribersCount() = Policy.arrangedSubscribersCount()
    [<Struct>]
    type ArrangedView<'K when 'K : comparison> internal (trace: Trace<'K>) =
        interface System.IDisposable with
            member _.Dispose() = Policy.decArranged()
        member _.Trace = trace
        member _.Iter(f: 'K -> int -> unit) = Trace.iter f trace
        member _.ToSeq() = Trace.toSeq trace
        member _.ToArray() = Trace.toSeq trace |> Seq.toArray
    
    let arrangedView (zset: ZSet<'K>) : ArrangedView<'K> =
        // Ensure arranged state by flushing overlays
        let z = flushAdaptiveInternal zset
        Policy.incArranged()
        new ArrangedView<'K>(z.Batch)

    let private selectedBackend =
        let v = System.Environment.GetEnvironmentVariable("DBSP_BACKEND")
        match v with
        | null -> ZSetBackend.Hash
        | s when s.Equals("Batch", StringComparison.OrdinalIgnoreCase) -> ZSetBackend.Batch
        | s when s.Equals("FastZSet", StringComparison.OrdinalIgnoreCase) || s.Equals("Fast", StringComparison.OrdinalIgnoreCase) -> ZSetBackend.Fast
        | s when s.Equals("HashMap", StringComparison.OrdinalIgnoreCase) || s.Equals("Hash", StringComparison.OrdinalIgnoreCase) -> ZSetBackend.Hash
        | s when s.Equals("Adaptive", StringComparison.OrdinalIgnoreCase) -> ZSetBackend.Adaptive
        | _ -> ZSetBackend.Hash

    // Internal: compact batches if too many; simple cap R
    let private compactIfNeeded (t: Trace<'K>) : Trace<'K> =
        if List.length t.Batches > DEFAULT_R then
            // fold left pairwise until under cap
            let mutable batches = t.Batches
            while List.length batches > DEFAULT_R do
                match batches with
                | a::b::rest -> batches <- (BatchTrace.Batch.union a b) :: rest
                | _ -> batches <- batches
            { Batches = batches |> List.filter (fun b -> b.Pairs.Length > 0); Cached = None }
        else t

    // Internal: reuse the helper for module-level ops
    let flushAdaptive (z: ZSet<'K>) : ZSet<'K> = flushAdaptiveInternal z

    /// Create empty ZSet
    let empty<'K when 'K : comparison> : ZSet<'K> =
        match selectedBackend with
        | ZSetBackend.Batch -> { Backend = ZSetBackend.Batch; Fast = FZ.empty 0; Batch = Trace.empty<'K>; Hash = HashMap.empty; Small = [||]; LastFlushTicks = 0L }
        | ZSetBackend.Fast -> { Backend = ZSetBackend.Fast; Fast = FZ.empty 0; Batch = Trace.empty<'K>; Hash = HashMap.empty; Small = [||]; LastFlushTicks = 0L }
        | ZSetBackend.Hash -> { Backend = ZSetBackend.Hash; Fast = FZ.empty 0; Batch = Trace.empty<'K>; Hash = HashMap.empty; Small = [||]; LastFlushTicks = 0L }
        | ZSetBackend.Adaptive -> { Backend = ZSetBackend.Adaptive; Fast = FZ.empty 0; Batch = Trace.empty<'K>; Hash = HashMap.empty; Small = [||]; LastFlushTicks = 0L }

    /// Create singleton ZSet with given key and weight
    let singleton key weight =
        if weight = 0 then empty
        else
        match selectedBackend with
        | ZSetBackend.Batch -> { Backend = ZSetBackend.Batch; Fast = FZ.empty 0; Batch = Trace.ofSeq [ (key, weight) ]; Hash = HashMap.empty; Small = [||]; LastFlushTicks = 0L }
        | ZSetBackend.Fast ->
            let d = FZ.empty 1
            if weight <> 0 then FZ.insertOrUpdate d key weight
            { Backend = ZSetBackend.Fast; Fast = d; Batch = Trace.empty<'K>; Hash = HashMap.empty; Small = [||]; LastFlushTicks = 0L }
        | ZSetBackend.Hash -> { Backend = ZSetBackend.Hash; Fast = FZ.empty 0; Batch = Trace.empty<'K>; Hash = HashMap.ofList [ (key, weight) ]; Small = [||]; LastFlushTicks = 0L }
        | ZSetBackend.Adaptive -> { Backend = ZSetBackend.Adaptive; Fast = FZ.empty 0; Batch = Trace.empty<'K>; Hash = HashMap.empty; Small = [| (key, weight) |]; LastFlushTicks = 0L }

    /// Create ZSet from sequence of key-weight pairs
    let ofSeq (pairs: seq<'K * int>) =
        match selectedBackend with
        | ZSetBackend.Batch -> { Backend = ZSetBackend.Batch; Fast = FZ.empty 0; Batch = Trace.ofSeq pairs; Hash = HashMap.empty; Small = [||]; LastFlushTicks = 0L }
        | ZSetBackend.Fast -> { Backend = ZSetBackend.Fast; Fast = FZ.ofSeq pairs; Batch = Trace.empty<'K>; Hash = HashMap.empty; Small = [||]; LastFlushTicks = 0L }
        | ZSetBackend.Hash -> { Backend = ZSetBackend.Hash; Fast = FZ.empty 0; Batch = Trace.empty<'K>; Hash = HashMap.ofSeq pairs; Small = [||]; LastFlushTicks = 0L }
        | ZSetBackend.Adaptive ->
            // Tiny-set optimization
            let arr = pairs |> Seq.filter (fun (_,w) -> w <> 0) |> Seq.toArray
            if arr.Length <= DEFAULT_N then
                // normalize and sort small array
                if arr.Length = 0 then { Backend = ZSetBackend.Adaptive; Fast = FZ.empty 0; Batch = Trace.empty<'K>; Hash = HashMap.empty; Small = [||]; LastFlushTicks = 0L }
                else
                let small = arr |> Array.sortBy fst
                // combine duplicates
                let mutable k = 0
                let mutable curK, curW = fst small.[0], snd small.[0]
                for i = 1 to small.Length - 1 do
                    let (kk, ww) = small.[i]
                    if kk = curK then curW <- curW + ww
                    else
                        if curW <> 0 then (small.[k] <- (curK, curW); k <- k + 1)
                        curK <- kk; curW <- ww
                if curW <> 0 then (small.[k] <- (curK, curW); k <- k + 1)
                let small = if k = 0 then [||] else small.[0..k-1]
                { Backend = ZSetBackend.Adaptive; Fast = FZ.empty 0; Batch = Trace.empty<'K>; Hash = HashMap.empty; Small = small; LastFlushTicks = 0L }
            else
                { Backend = ZSetBackend.Adaptive; Fast = FZ.ofSeq arr; Batch = Trace.empty<'K>; Hash = HashMap.empty; Small = [||]; LastFlushTicks = 0L }

    /// Create ZSet from list of key-weight pairs
    let ofList (pairs: ('K * int) list) = ofSeq pairs

    /// Efficient batch insertion
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let insertMany (zset: ZSet<'K>) (pairs: seq<'K * int>) =
        match zset.Backend with
        | ZSetBackend.Batch -> { zset with Batch = Trace.union zset.Batch (Trace.ofSeq pairs) }
        | ZSetBackend.Fast -> { zset with Fast = FZ.union zset.Fast (FZ.ofSeq pairs) }
        | ZSetBackend.Hash ->
            let additions = HashMap.ofSeq pairs
            let merged = HashMap.unionWith (fun _ w1 w2 -> let s = w1 + w2 in if s = 0 then 0 else s) zset.Hash additions
            { zset with Hash = merged }
        | ZSetBackend.Adaptive ->
            // Append to small vector if tiny, else to memtable
            let arr = pairs |> Seq.filter (fun (_,w) -> w <> 0) |> Seq.toArray
            if zset.Small.Length + arr.Length <= DEFAULT_N then
                // merge arrays and re-normalize
                let merged = Array.append zset.Small arr |> Array.sortBy fst
                // combine duplicates
                if merged.Length = 0 then { zset with Small = [||] } else
                let mutable k = 0
                let mutable curK, curW = fst merged.[0], snd merged.[0]
                for i = 1 to merged.Length - 1 do
                    let (kk, ww) = merged.[i]
                    if kk = curK then curW <- curW + ww
                    else
                        if curW <> 0 then (merged.[k] <- (curK, curW); k <- k + 1)
                        curK <- kk; curW <- ww
                if curW <> 0 then (merged.[k] <- (curK, curW); k <- k + 1)
                let merged = if k = 0 then [||] else merged.[0..k-1]
                { zset with Small = merged } |> normalizeAdaptiveInternal
            else
                let mem = FZ.union zset.Fast (FZ.ofSeq arr)
                { zset with Fast = mem } |> normalizeAdaptiveInternal

    /// Builder for efficient ZSet construction
    type ZSetBuilder<'K when 'K : comparison>(?capacity: int) =
        let initialCap = defaultArg capacity 128
        let pairs = System.Collections.Generic.List<'K * int>(initialCap)
        member _.Add(key: 'K, weight: int) = if weight <> 0 then pairs.Add((key, weight))
        member this.AddRange(ps: seq<'K * int>) = for (k,w) in ps do this.Add(k,w)
        member _.Reserve(n: int) = if n > pairs.Capacity then pairs.Capacity <- n
        member _.Build() =
            let arr = pairs.ToArray()
            match selectedBackend with
            | ZSetBackend.Batch -> { Backend = ZSetBackend.Batch; Fast = FZ.empty 0; Batch = Trace.ofSeq arr; Hash = HashMap.empty; Small = [||]; LastFlushTicks = 0L }
            | ZSetBackend.Fast -> { Backend = ZSetBackend.Fast; Fast = FZ.ofSeq arr; Batch = Trace.empty<'K>; Hash = HashMap.empty; Small = [||]; LastFlushTicks = 0L }
            | ZSetBackend.Hash -> { Backend = ZSetBackend.Hash; Fast = FZ.empty 0; Batch = Trace.empty<'K>; Hash = HashMap.ofArray arr; Small = [||]; LastFlushTicks = 0L }
            | ZSetBackend.Adaptive ->
                if arr.Length <= DEFAULT_N then
                    // Normalize
                    let pairs = arr |> Array.filter (fun (_,w) -> w <> 0) |> Array.sortBy fst
                    let mutable k = 0
                    if pairs.Length = 0 then { Backend = ZSetBackend.Adaptive; Fast = FZ.empty 0; Batch = Trace.empty<'K>; Hash = HashMap.empty; Small = [||]; LastFlushTicks = 0L }
                    else
                        let mutable curK, curW = fst pairs.[0], snd pairs.[0]
                        for i = 1 to pairs.Length - 1 do
                            let (kk, ww) = pairs.[i]
                            if kk = curK then curW <- curW + ww
                            else
                                if curW <> 0 then (pairs.[k] <- (curK, curW); k <- k + 1)
                                curK <- kk; curW <- ww
                        if curW <> 0 then (pairs.[k] <- (curK, curW); k <- k + 1)
                        let small = if k = 0 then [||] else pairs.[0..k-1]
                        { Backend = ZSetBackend.Adaptive; Fast = FZ.empty 0; Batch = Trace.empty<'K>; Hash = HashMap.empty; Small = small; LastFlushTicks = 0L }
                else
                    { Backend = ZSetBackend.Adaptive; Fast = FZ.ofSeq arr; Batch = Trace.empty<'K>; Hash = HashMap.empty; Small = [||]; LastFlushTicks = 0L }

    /// Build ZSet efficiently using builder pattern
    let buildZSet (builderFn: ZSetBuilder<'K> -> unit) =
        let builder = ZSetBuilder<'K>()
        builderFn builder
        builder.Build()

    /// Optimized builder entry used by fused operators and benchmarks
    let buildWith (builderFn: ZSetBuilder<'K> -> unit) = buildZSet builderFn

    /// Convert ZSet to sequence of key-weight pairs (excluding zero weights)
    let toSeq (zset: ZSet<'K>) =
        match zset.Backend with
        | ZSetBackend.Batch -> Trace.toSeq zset.Batch
        | ZSetBackend.Fast -> FZ.toSeq zset.Fast
        | ZSetBackend.Hash -> HashMap.toSeq zset.Hash |> Seq.filter (fun (_,w) -> w <> 0)
        | ZSetBackend.Adaptive ->
            let z = normalizeAdaptiveInternal zset
            Trace.toSeq z.Batch

    /// Iterate without allocations using struct enumerator
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let iter (f: 'K -> int -> unit) (zset: ZSet<'K>) =
        match zset.Backend with
        | ZSetBackend.Batch -> Trace.iter f zset.Batch
        | ZSetBackend.Fast -> FZ.iter f zset.Fast
        | ZSetBackend.Hash -> HashMap.iter (fun k w -> if w <> 0 then f k w) zset.Hash
        | ZSetBackend.Adaptive ->
            let z = normalizeAdaptiveInternal zset
            Trace.iter f z.Batch

    /// Frozen snapshot for read-mostly enumeration
    let toArray (zset: ZSet<'K>) = (toSeq zset |> Seq.toArray)

    /// Check if key exists
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let containsKey (key: 'K) (zset: ZSet<'K>) =
        match zset.Backend with
        | ZSetBackend.Batch -> Trace.toSeq zset.Batch |> Seq.exists (fun (k,_) -> k = key)
        | ZSetBackend.Fast -> FZ.containsKey zset.Fast key
        | ZSetBackend.Hash -> HashMap.containsKey key zset.Hash
        | ZSetBackend.Adaptive ->
            // Probe small, then mem, then batches
            let smallHas =
                if zset.Small.Length = 0 then false else
                let arr = zset.Small
                let mutable lo, hi = 0, arr.Length - 1
                let mutable found = false
                while lo <= hi && not found do
                    let mid = (lo + hi) >>> 1
                    let (k,_) = arr.[mid]
                    if k = key then found <- true
                    elif k < key then lo <- mid + 1 else hi <- mid - 1
                found
            smallHas || FZ.containsKey zset.Fast key || (zset.Batch.Batches |> List.exists (fun b -> BatchTrace.Batch.tryFind key b |> Option.isSome))

    /// Try get weight
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let tryFind (key: 'K) (zset: ZSet<'K>) =
        match zset.Backend with
        | ZSetBackend.Batch -> Trace.toSeq zset.Batch |> Seq.tryPick (fun (k,w) -> if k = key then Some w else None)
        | ZSetBackend.Fast -> FZ.tryFind zset.Fast key
        | ZSetBackend.Hash -> HashMap.tryFind key zset.Hash
        | ZSetBackend.Adaptive ->
            let w = zset.GetWeight key
            if w = 0 then None else Some w

    /// Union many ZSets with pre-sizing based on key counts
    let unionMany (sets: seq<ZSet<'K>>) =
        match selectedBackend with
        | ZSetBackend.Batch -> sets |> Seq.fold (fun (acc: ZSet<'K>) z -> { acc with Batch = Trace.union acc.Batch z.Batch }) (empty)
        | ZSetBackend.Fast ->
            let totalKeys = sets |> Seq.fold (fun acc z -> acc + z.Fast.Count) 0
            let initialCap = max 16 (1 <<< (32 - System.Numerics.BitOperations.LeadingZeroCount(uint32 (max 1 (totalKeys * 2 - 1)))))
            let dict = FZ.empty<'K> initialCap
            for z in sets do iter (fun k w -> if w <> 0 then FZ.insertOrUpdate dict k w) z
            { Backend = ZSetBackend.Fast; Fast = dict; Batch = Trace.empty<'K>; Hash = HashMap.empty; Small = [||]; LastFlushTicks = 0L }
        | ZSetBackend.Hash ->
            let merged =
                sets
                |> Seq.map (fun z -> z.Hash)
                |> Seq.fold (fun acc h -> HashMap.unionWith (fun _ w1 w2 -> let s = w1 + w2 in if s = 0 then 0 else s) acc h) HashMap.empty
            { Backend = ZSetBackend.Hash; Fast = FZ.empty 0; Batch = Trace.empty<'K>; Hash = merged; Small = [||]; LastFlushTicks = 0L }
        | ZSetBackend.Adaptive ->
            // Build a new Adaptive set by merging memtables/smalls and appending traces
            let mutable totalMem = 0
            let mutable totalSmall = 0
            let mutable combinedTrace : Trace<'K> = Trace.empty
            // first pass: sizes and trace concat
            for z in sets do
                totalMem <- totalMem + z.Fast.Count
                totalSmall <- totalSmall + z.Small.Length
                combinedTrace <- Trace.union combinedTrace z.Batch
            let mem = FZ.empty<'K> (max 16 (totalMem + totalSmall))
            // second pass: fill mem with smalls + mem overlays
            for z in sets do
                for (k,w) in z.Small do if w <> 0 then FZ.insertOrUpdate mem k w
                FZ.iter (fun k w -> if w <> 0 then FZ.insertOrUpdate mem k w) z.Fast
            { Backend = ZSetBackend.Adaptive; Fast = mem; Batch = compactIfNeeded combinedTrace; Hash = HashMap.empty; Small = [||]; LastFlushTicks = System.DateTime.UtcNow.Ticks }

    /// Convenient inline functions using F# 7+ simplified SRTP syntax
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let inline add (zset1: ZSet<'K>) (zset2: ZSet<'K>) = zset1 + zset2

    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let inline negate (zset: ZSet<'K>) = -zset

    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let inline scalarMultiply scalar (zset: ZSet<'K>) = scalar * zset

    /// Insert a key with given weight (adds to existing weight)
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let insert key weight (zset: ZSet<'K>) =
        match zset.Backend with
        | ZSetBackend.Batch -> { zset with Batch = Trace.union zset.Batch (Trace.ofSeq [ (key, weight) ]) }
        | ZSetBackend.Fast ->
            let d = FZ.empty 1
            if weight <> 0 then FZ.insertOrUpdate d key weight
            { zset with Fast = FZ.union zset.Fast d }
        | ZSetBackend.Hash ->
            let merged =
                HashMap.alter key (fun opt ->
                    match opt with
                    | None -> if weight = 0 then None else Some weight
                    | Some w -> let s = w + weight in if s = 0 then None else Some s)
                    zset.Hash
            { zset with Hash = merged }
        | ZSetBackend.Adaptive ->
            if weight = 0 then zset else
            // if tiny-set still within threshold, keep in small; else memtable
            if zset.Small.Length + 1 <= DEFAULT_N then
                // insert into small array: linear insert is fine for tiny sizes
                let arr = Array.zeroCreate<'K * int> (zset.Small.Length + 1)
                Array.blit zset.Small 0 arr 0 zset.Small.Length
                arr.[arr.Length - 1] <- (key, weight)
                // normalize small
                let arr = arr |> Array.filter (fun (_,w) -> w <> 0) |> Array.sortBy fst
                let mutable k = 0
                if arr.Length = 0 then { zset with Small = [||] } else
                let mutable curK, curW = fst arr.[0], snd arr.[0]
                for i = 1 to arr.Length - 1 do
                    let (kk, ww) = arr.[i]
                    if kk = curK then curW <- curW + ww
                    else (if curW <> 0 then (arr.[k] <- (curK, curW); k <- k + 1); curK <- kk; curW <- ww)
                if curW <> 0 then (arr.[k] <- (curK, curW); k <- k + 1)
                let small = if k = 0 then [||] else arr.[0..k-1]
                { zset with Small = small }
            else
                let mem = zset.Fast
                FZ.insertOrUpdate mem key weight
                // Optional: flush on size bound exceeded
                let nowTicks = System.DateTime.UtcNow.Ticks
                let elapsedMs = if zset.LastFlushTicks = 0L then System.Int64.MaxValue else int ((nowTicks - zset.LastFlushTicks) / System.TimeSpan.TicksPerMillisecond)
                let subs = arrangedSubscribersCount()
                let tMs = if subs > 0 then max 1 (DEFAULT_T_MS / 4) else DEFAULT_T_MS
                let sTh = if subs > 0 then max 1024 (DEFAULT_S / 4) else DEFAULT_S
                let shouldFlush = (mem.Count + zset.Small.Length) >= sTh || elapsedMs >= tMs
                if shouldFlush then flushAdaptive { zset with Fast = mem } else { zset with Fast = mem }

    /// Remove a key by adding negative weight
    let remove key weight zset = insert key (-weight) zset

    /// Union of two ZSets (same as addition)
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let inline union zset1 zset2 = add zset1 zset2

    /// Difference of two ZSets
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let inline difference zset1 zset2 = add zset1 (negate zset2)

    /// Filter ZSet by key predicate
    let filter (predicate) zset =
        let res = buildWith (fun b ->
            for (k,w) in toSeq zset do
                if w <> 0 && predicate k then b.Add(k, w)
        )
        match selectedBackend with
        | ZSetBackend.Adaptive -> normalizeAdaptiveInternal res
        | _ -> res

    /// Map over keys (preserving weights)
    let mapKeys (f) zset =
        let res = toSeq zset |> Seq.map (fun (k,w) -> (f k, w)) |> ofSeq
        match selectedBackend with
        | ZSetBackend.Adaptive -> normalizeAdaptiveInternal res
        | _ -> res

    /// Fold over ZSet entries without allocating sequences
    let fold folder state zset =
        let mutable acc = state
        iter (fun k w -> if w <> 0 then acc <- folder acc k w) zset
        acc
