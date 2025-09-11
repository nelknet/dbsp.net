namespace DBSP.Tests.Performance

open System
open BenchmarkDotNet.Attributes
open BenchmarkDotNet.Jobs
open DBSP.Core.ZSet
open DBSP.Core.IndexedZSet
open DBSP.Operators.JoinOperators
open DBSP.Operators.FusedOperators
open DBSP.Operators.Interfaces

type L = { Id:int; Cid:int; Pid:int; Qty:int; Price:int }
type R = { Pid:int; Class:string }

[<SimpleJob(RuntimeMoniker.Net90)>]
[<MemoryDiagnoser>]
type JoinProjectionBenchmarks() =

    [<Params(100000)>]
    member val DataSize = 0 with get, set

    [<Params(1, 100)>]
    member val ChangeCount = 0 with get, set

    member val private baseLeft : L array = Array.empty with get, set
    member val private rightDim : R array = Array.empty with get, set

    // Operators
    member val private joinOp : IBinaryOperator<ZSet<int * (int * int)>, ZSet<int * string>, IndexedZSet<int, (int * int) * string>> =
        (InnerJoinOperator<int, (int * int), string>() :> IBinaryOperator<ZSet<int * (int * int)>, ZSet<int * string>, IndexedZSet<int, (int * int) * string>>) with get, set
    member val private fusedOp : IBinaryOperator<ZSet<int * (int * int)>, ZSet<int * string>, ZSet<(int * string) * int>> =
        (JoinProjectOperator<int, (int * (int * int)), (int * string), (int * string) * int>(
        (fun (pid, (_)) -> pid),
        (fun (pid, _) -> pid),
        (fun ((_, (cid, amt))) (_, cls) -> ((cid, cls), amt))
        ) :> IBinaryOperator<ZSet<int * (int * int)>, ZSet<int * string>, ZSet<(int * string) * int>>) with get, set

    member val private rightZ : ZSet<int * string> = ZSet.empty with get, set

    [<GlobalSetup>]
    member this.Setup() =
        let rnd = Random(42)
        // Base left: shape (cid, amount) keyed by pid
        this.baseLeft <- Array.init this.DataSize (fun i ->
            { Id = i; Cid = rnd.Next(this.DataSize/10+1); Pid = rnd.Next(10000); Qty = 1 + rnd.Next(5); Price = 100 + rnd.Next(5000) })
        // Right dimension: pid -> class
        this.rightDim <- Array.init 10000 (fun pid -> { Pid=pid; Class = if pid % 2 = 0 then "A" else "B" })
        this.rightZ <- ZSet.ofSeq (seq { for r in this.rightDim -> ((r.Pid, r.Class), 1) }) |> ZSet.mapKeys id
        // Preload right state in both operators
        let _ = (this.joinOp.EvalAsync (ZSet.empty<int * (int * int)>) this.rightZ).Result in ()
        let _ = (this.fusedOp.EvalAsync (ZSet.empty<int * (int * int)>) this.rightZ).Result in ()

    member this.MakeDelta() =
        let rnd = Random(7)
        let changes = Array.init this.ChangeCount (fun _ ->
            let idx = rnd.Next this.DataSize
            let o = this.baseLeft[idx]
            { o with Qty = o.Qty + rnd.Next(0,2); Price = o.Price + rnd.Next(-10,10) })
        // Convert to ZSet of left values (pid keyed left will be computed in join)
        let leftDelta = ZSet.ofSeq (seq {
            for n in changes do
                let old = this.baseLeft.[n.Id]
                let oldAmt = old.Qty * old.Price
                let newAmt = n.Qty * n.Price
                // value is (cid, amount) and join key is pid
                yield ((old.Pid, (old.Cid, oldAmt)), -1)
                yield ((n.Pid, (n.Cid, newAmt)), 1)
        })
        leftDelta

    [<Benchmark(Baseline = true)>]
    member this.Baseline_Join_Then_Project() =
        let leftDelta = this.MakeDelta()
        let joined = (this.joinOp.EvalAsync leftDelta (ZSet.empty<int * string>)).Result |> IndexedZSet.toZSet
        // project to ((cid,class), amount)
        ZSet.fold (fun acc (pid, ((cid, amt), cls)) w -> ZSet.insert ((cid, cls), amt) w acc) (ZSet.empty<(int*string)*int>) joined

    [<Benchmark>]
    member this.Fused_JoinProject() =
        let leftDelta = this.MakeDelta()
        (this.fusedOp.EvalAsync leftDelta (ZSet.empty<int * string>)).Result
