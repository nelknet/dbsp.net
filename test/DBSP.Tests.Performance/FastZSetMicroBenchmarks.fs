namespace DBSP.Tests.Performance

open System
open BenchmarkDotNet.Attributes
open DBSP.Core.ZSet

[<MemoryDiagnoser>]
type FastZSetMicroBenchmarks() =

    [<Params(1000, 10000)>]
    member val Size = 0 with get, set

    [<Params(100)>]
    member val Delta = 0 with get, set

    member val private zs : ZSet<int> = ZSet.empty with get, set
    member val private keys : int[] = Array.empty with get, set

    [<GlobalSetup>]
    member this.Setup() =
        let rnd = Random 42
        let pairs = seq { for i in 0 .. this.Size - 1 -> (i, 1) }
        this.zs <- ZSet.ofSeq pairs
        this.keys <- Array.init this.Delta (fun _ -> rnd.Next(0, this.Size))

    [<Benchmark(Baseline=true)>]
    member this.ToSeq_Enumerate() =
        // Enumerate all entries using toSeq
        let mutable s = 0
        for (k,w) in ZSet.toSeq this.zs do
            s <- s + (if w <> 0 then 1 else 0)
        s

    [<Benchmark>]
    member this.Iter_Enumerate() =
        let mutable s = 0
        ZSet.iter (fun _ w -> if w <> 0 then s <- s + 1) this.zs
        s

    [<Benchmark>]
    member this.ContainsKey_Random() =
        let mutable c = 0
        for k in this.keys do if ZSet.containsKey k this.zs then c <- c + 1
        c

    [<Benchmark>]
    member this.TryFind_Random() =
        let mutable c = 0
        for k in this.keys do match ZSet.tryFind k this.zs with | Some _ -> c <- c + 1 | _ -> ()
        c

    [<Benchmark>]
    member this.UnionMany_Deltas() =
        // Build many small delta ZSets and union them into a result
        let deltas =
            [| for i in 0 .. this.Delta - 1 -> ZSet.ofList [ (i % this.Size, -1); ((i + 1) % this.Size, 1) ] |]
        Array.fold ZSet.union this.zs deltas

