module DBSP.Operators.WindowOperators

open System
open System.Collections.Generic
open System.Runtime.CompilerServices
open System.Threading.Tasks
open DBSP.Core.ZSet
open DBSP.Operators.Interfaces

/// Event with timestamp abstraction
[<Struct>]
type Timestamped<'T> = { Ts: int64; Value: 'T }

/// Simple tumbling window aggregator with watermark
type TumblingWindow<'T,'K,'Agg when 'T: comparison and 'K: comparison and 'Agg: comparison>(
    ts: 'T -> int64,
    key: 'T -> 'K,
    windowSize: int64,
    allowedLateness: int64,
    aggregate: 'Agg -> 'T -> 'Agg,
    initial: 'Agg,
    ?name: string) =
    inherit BaseUnaryOperator<ZSet<'T>, ZSet<'K * 'Agg>>(defaultArg name "TumblingWindow")

    // state: per-bucket aggregates, watermark
    let buckets = Dictionary<int64, Dictionary<'K,'Agg>>()
    let mutable watermark = Int64.MinValue

    let getBucketId (t:int64) = (t / windowSize) * windowSize

    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    override _.EvalAsyncImpl(input: ZSet<'T>) = task {
        // update watermark as max seen timestamp - allowedLateness
        for (v, w) in ZSet.toSeq input do
            if w <> 0 then watermark <- max watermark (ts v)
        let cutoff = watermark - allowedLateness

        // apply updates into bucket aggregates
        for (v, w) in ZSet.toSeq input do
            let t = ts v
            let b = getBucketId t
            if not (buckets.ContainsKey b) then buckets[b] <- Dictionary<'K,'Agg>()
            let bagg = buckets[b]
            let k = key v
            let current = if bagg.ContainsKey k then bagg[k] else initial
            // collapse weights by applying aggregate |w| times; for weighted zsets assume weights are small
            let mutable acc = current
            let times = abs w
            for _i in 1 .. times do acc <- aggregate acc v
            // if weight negative, we cannot "un-aggregate" generally; in practice use groups of increments only in this example
            bagg[k] <- acc

        // emit ready buckets (window end <= cutoff)
        let ready =
            buckets.Keys
            |> Seq.filter (fun b -> b + windowSize <= cutoff)
            |> Seq.toArray
        let outputs = ResizeArray<'K * 'Agg>()
        for b in ready do
            for kv in buckets[b] do
                outputs.Add(kv.Key, kv.Value)
            buckets.Remove b |> ignore
        // return as zset with +1 weight
        let z = outputs |> Seq.map (fun (k,a) -> (k,a), 1) |> ZSet.ofSeq
        return z
    }

/// Row-based sliding window of last N elements per key (simple count window)
type SlidingCountWindow<'T,'K when 'T: comparison and 'K: comparison>(
    key: 'T -> 'K,
    size: int,
    ?name: string) =
    inherit BaseUnaryOperator<ZSet<'T>, ZSet<'K * int>>(defaultArg name "SlidingCountWindow")

    let buffers = Dictionary<'K, System.Collections.Generic.Queue<'T>>()

    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    override _.EvalAsyncImpl(input: ZSet<'T>) = task {
        for (v, w) in ZSet.toSeq input do
            let k = key v
            if not (buffers.ContainsKey k) then buffers[k] <- System.Collections.Generic.Queue<'T>()
            let q = buffers[k]
            let times = abs w
            for _i in 1 .. times do
                if w > 0 then q.Enqueue v else if q.Count > 0 then q.Dequeue() |> ignore
            while q.Count > size do q.Dequeue() |> ignore
        let out =
            buffers
            |> Seq.map (fun kv -> ((kv.Key, kv.Value.Count), 1))
            |> ZSet.ofSeq
        return out
    }
