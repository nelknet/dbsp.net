module DBSP.Tests.Unit.WindowOperatorTests

open NUnit.Framework
open DBSP.Operators.WindowOperators
open DBSP.Core.ZSet

[<Test>]
let ``Tumbling window aggregates and emits on watermark`` () =
    // Events with timestamps 0..29, window 10, lateness 0; aggregate count per bucket by key string
    let ts (t:int * string) = int64 (fst t)
    let key (_t:int*string) = snd _t
    let agg acc (_:int*string) = acc + 1
    let win = TumblingWindow(ts, key, 10L, 0L, agg, 0)
    let evs = [ for i in 0 .. 29 -> ((i, if i<10 then "A" elif i<20 then "B" else "C"), 1) ] |> ZSet.ofSeq
    let out = win.EvalAsyncImpl(evs) |> Async.AwaitTask |> Async.RunSynchronously
    let seq = ZSet.toSeq out |> Seq.toArray
    // Expect buckets [0,10), [10,20) emitted because watermark is 29 and lateness 0; [20,30) also emitted (end 30 <= 29? No) -> only first two
    let counts = seq |> Array.groupBy (fun ((k,_),_) -> k) |> Array.map (fun (k,arr) -> k, Array.sumBy (fun ((_k,c),_) -> c) arr)
    Assert.That(counts |> Array.exists (fun (k,c) -> k="A" && c=10), Is.True)
    Assert.That(counts |> Array.exists (fun (k,c) -> k="B" && c=10), Is.True)

[<Test>]
let ``Sliding count window keeps last N per key`` () =
    let key (i:int) = if i % 2 = 0 then "E" else "O"
    let win = SlidingCountWindow(key, 3)
    let evs = [ for i in 1 .. 5 -> (i, 1) ] |> ZSet.ofSeq
    let out = win.EvalAsyncImpl(evs) |> Async.AwaitTask |> Async.RunSynchronously
    let arr = ZSet.toSeq out |> Seq.toArray
    // Expect E has 2 evens <=5 in last 3 -> 2; O has 3 odds in last 3 -> 3
    Assert.That(arr |> Array.exists (fun ((k,c),_) -> k="E" && c=2), Is.True)
    Assert.That(arr |> Array.exists (fun ((k,c),_) -> k="O" && c=3), Is.True)

