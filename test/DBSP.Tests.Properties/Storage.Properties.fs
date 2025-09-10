module DBSP.Tests.Properties.StorageProperties

open FsCheck
open FsCheck.FSharp
open FsCheck.NUnit
open NUnit.Framework
open DBSP.Storage
open System
open System.Collections.Generic

module StorageGenerators =
    let updateGen =
        Gen.listOfLength 50 <|
            gen {
                let! k = Gen.choose (0, 20)
                let! v = Gen.choose (-5, 5)
                let! w = Gen.choose (-3, 3)
                return (k, v, int64 w)
            }
    let updateGenQuick =
        Gen.listOfLength 20 <|
            gen {
                let! k = Gen.choose (0, 20)
                let! v = Gen.choose (-5, 5)
                let! w = Gen.choose (-3, 3)
                return (k, v, int64 w)
            }

type StorageArbs =
    static member Updates() : Arbitrary<(int*int*int64) list> =
        Arb.fromGen StorageGenerators.updateGen
type StorageArbsQuick =
    static member Updates() : Arbitrary<(int*int*int64) list> =
        Arb.fromGen StorageGenerators.updateGenQuick

type TemporalArbs =
    static member UpdatesByTime() : Arbitrary<(int64 * (int*int*int64) list) list> =
        let timeGen =
            Gen.listOfLength 5 <|
                gen {
                    let! t = Gen.choose (0, 10)
                    let! ups = StorageGenerators.updateGen
                    return (int64 t, ups)
                }
        Arb.fromGen timeGen

[<FsCheck.NUnit.Property(MaxTest = 30)>]
[<Category("Slow")>]
let ``LSMStorage matches expected (K,V)->weight aggregation semantics`` (updates: (int*int*int64) list) =
    // Arrange
    let tmp = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "dbsp_prop_lsm_" + string (System.Guid.NewGuid()))
    System.IO.Directory.CreateDirectory(tmp) |> ignore
    let config = { DataPath = tmp; MaxMemoryBytes = 10_000_000L; CompactionThreshold = 64; WriteBufferSize = 16_384; BlockCacheSize = 1_000_000L; SpillThreshold = 0.8 }
    let serKV = MessagePackSerializer<System.ValueTuple<int,int>>() :> ISerializer<System.ValueTuple<int,int>>
    let lsm = LSMStorageBackend<int,int>(config, serKV)

    // Act: write updates to LSM
    (lsm.StoreBatchWithFlush(updates, true)).GetAwaiter().GetResult()
    let lsmRaw = ((lsm :> IStorageBackend<int,int>).GetIterator()).GetAwaiter().GetResult() |> Seq.toArray

    // Coalesce any duplicate (K,V) that may appear across segments
    let lsmDict = Dictionary<(int*int), int64>()
    for (k,v,w) in lsmRaw do
        let key = (k,v)
        if lsmDict.ContainsKey(key) then lsmDict[key] <- lsmDict[key] + w else lsmDict[key] <- w
    let lsmCoalesced =
        lsmDict
        |> Seq.choose (fun kvp -> let (k,v) = kvp.Key in let w = kvp.Value in if w = 0L then None else Some (k,v,w))
        |> Seq.toArray
        |> Array.sort

    // Expected aggregation by (K,V)
    let expectedDict = Dictionary<(int*int), int64>()
    for (k,v,w) in updates do
        let key = (k,v)
        if expectedDict.ContainsKey(key) then expectedDict[key] <- expectedDict[key] + w else expectedDict[key] <- w
    let expected =
        expectedDict
        |> Seq.choose (fun kvp -> let (k,v) = kvp.Key in let w = kvp.Value in if w = 0L then None else Some (k,v,w))
        |> Seq.toArray
        |> Array.sort
    // Assert: identical non-zero entries
    lsmCoalesced = expected

[<FsCheck.NUnit.Property(MaxTest = 30)>]
[<Category("Slow")>]
let ``Multi-batch application is equivalent to single-batch`` (updates: (int*int*int64) list) =
    // Arrange
    let tmp1 = IO.Path.Combine(IO.Path.GetTempPath(), "dbsp_prop_multibatch1_" + string (Guid.NewGuid()))
    let tmp2 = IO.Path.Combine(IO.Path.GetTempPath(), "dbsp_prop_multibatch2_" + string (Guid.NewGuid()))
    IO.Directory.CreateDirectory(tmp1) |> ignore
    IO.Directory.CreateDirectory(tmp2) |> ignore
    let cfg1 = { DataPath = tmp1; MaxMemoryBytes = 10_000_000L; CompactionThreshold = 64; WriteBufferSize = 16_384; BlockCacheSize = 1_000_000L; SpillThreshold = 0.8 }
    let cfg2 = { DataPath = tmp2; MaxMemoryBytes = 10_000_000L; CompactionThreshold = 64; WriteBufferSize = 16_384; BlockCacheSize = 1_000_000L; SpillThreshold = 0.8 }
    let serKV = MessagePackSerializer<System.ValueTuple<int,int>>() :> ISerializer<System.ValueTuple<int,int>>
    let lsmSingle = LSMStorageBackend<int,int>(cfg1, serKV)
    let lsmMulti = LSMStorageBackend<int,int>(cfg2, serKV)

    // Single-batch
    (lsmSingle.StoreBatchWithFlush(updates, true)).GetAwaiter().GetResult()

    // Multi-batch: split into random-sized chunks
    let rnd = Random(42)
    let mutable rest = updates
    while not rest.IsEmpty do
        let take = 1 + rnd.Next(0, 5)
        let chunk = rest |> List.truncate take
        rest <- rest |> List.skip chunk.Length
        (lsmMulti.StoreBatchWithFlush(chunk, true)).GetAwaiter().GetResult()

    // Compare
    let leftRaw = ((lsmSingle :> IStorageBackend<int,int>).GetIterator()).GetAwaiter().GetResult() |> Seq.toArray
    let rightRaw = ((lsmMulti :> IStorageBackend<int,int>).GetIterator()).GetAwaiter().GetResult() |> Seq.toArray
    let coalesce (arr: (int*int*int64) array) =
        let d = Dictionary<(int*int), int64>()
        for (k,v,w) in arr do
            let key = (k,v)
            if d.ContainsKey(key) then d[key] <- d[key] + w else d[key] <- w
        d
        |> Seq.choose (fun kvp -> let (k,v) = kvp.Key in let w = kvp.Value in if w = 0L then None else Some (k,v,w))
        |> Seq.toArray
        |> Array.sort
    let left = coalesce leftRaw
    let right = coalesce rightRaw
    left = right

[<Test>]
[<Category("Stress")>]
[<Explicit("Nightly stress profile")>]
let ``Multi-batch application is equivalent to single-batch (stress)`` () =
    let property (updates: (int*int*int64) list) =
        let tmp1 = IO.Path.Combine(IO.Path.GetTempPath(), "dbsp_prop_multibatch1_stress_" + string (Guid.NewGuid()))
        let tmp2 = IO.Path.Combine(IO.Path.GetTempPath(), "dbsp_prop_multibatch2_stress_" + string (Guid.NewGuid()))
        IO.Directory.CreateDirectory(tmp1) |> ignore
        IO.Directory.CreateDirectory(tmp2) |> ignore
        let cfg1 = { DataPath = tmp1; MaxMemoryBytes = 10_000_000L; CompactionThreshold = 16; WriteBufferSize = 1024; BlockCacheSize = 1_000_000L; SpillThreshold = 0.8 }
        let cfg2 = { DataPath = tmp2; MaxMemoryBytes = 10_000_000L; CompactionThreshold = 16; WriteBufferSize = 1024; BlockCacheSize = 1_000_000L; SpillThreshold = 0.8 }
        let serKV = MessagePackSerializer<System.ValueTuple<int,int>>() :> ISerializer<System.ValueTuple<int,int>>
        let lsmSingle = LSMStorageBackend<int,int>(cfg1, serKV)
        let lsmMulti = LSMStorageBackend<int,int>(cfg2, serKV)
        (lsmSingle.StoreBatchWithFlush(updates, true)).GetAwaiter().GetResult()
        let rnd = Random(42)
        let mutable rest = updates
        while not rest.IsEmpty do
            let take = 1 + rnd.Next(0, 5)
            let chunk = rest |> List.truncate take
            rest <- rest |> List.skip chunk.Length
            (lsmMulti.StoreBatchWithFlush(chunk, true)).GetAwaiter().GetResult()
        let leftRaw = ((lsmSingle :> IStorageBackend<int,int>).GetIterator()).GetAwaiter().GetResult() |> Seq.toArray
        let rightRaw = ((lsmMulti :> IStorageBackend<int,int>).GetIterator()).GetAwaiter().GetResult() |> Seq.toArray
        let coalesce (arr: (int*int*int64) array) =
            let d = Dictionary<(int*int), int64>()
            for (k,v,w) in arr do
                let key = (k,v)
                if d.ContainsKey(key) then d[key] <- d[key] + w else d[key] <- w
            d
            |> Seq.choose (fun kvp -> let (k,v) = kvp.Key in let w = kvp.Value in if w = 0L then None else Some (k,v,w))
            |> Seq.toArray
            |> Array.sort
        let left = coalesce leftRaw
        let right = coalesce rightRaw
        left = right
    Check.One(Config.Quick, Prop.forAll (Arb.fromGen StorageGenerators.updateGen) property)

open NUnit.Framework

[<TestFixture>]
type StoragePropertyTests_Remainder() =
    
    [<FsCheck.NUnit.Property(MaxTest = 30, Arbitrary = [| typeof<StorageArbsQuick> |])>]
    [<Category("Slow")>]
    member _.``Order of updates does not affect final state``(updates: (int*int*int64) list) =
        let tmp1 = IO.Path.Combine(IO.Path.GetTempPath(), "dbsp_prop_order1_" + string (Guid.NewGuid()))
        let tmp2 = IO.Path.Combine(IO.Path.GetTempPath(), "dbsp_prop_order2_" + string (Guid.NewGuid()))
        IO.Directory.CreateDirectory(tmp1) |> ignore
        IO.Directory.CreateDirectory(tmp2) |> ignore
        let cfg1 = { DataPath = tmp1; MaxMemoryBytes = 10_000_000L; CompactionThreshold = 64; WriteBufferSize = 16_384; BlockCacheSize = 1_000_000L; SpillThreshold = 0.8 }
        let cfg2 = { DataPath = tmp2; MaxMemoryBytes = 10_000_000L; CompactionThreshold = 64; WriteBufferSize = 16_384; BlockCacheSize = 1_000_000L; SpillThreshold = 0.8 }
        let serKV = MessagePackSerializer<System.ValueTuple<int,int>>() :> ISerializer<System.ValueTuple<int,int>>
        let lsmA = LSMStorageBackend<int,int>(cfg1, serKV)
        let lsmB = LSMStorageBackend<int,int>(cfg2, serKV)
        (lsmA.StoreBatchWithFlush(updates, true)).GetAwaiter().GetResult()
        let shuffled = updates |> List.sortBy (fun _ -> Guid.NewGuid())
        (lsmB.StoreBatchWithFlush(shuffled, true)).GetAwaiter().GetResult()
        let a = ((lsmA :> IStorageBackend<int,int>).GetIterator()).GetAwaiter().GetResult() |> Seq.toArray |> Array.sort
        let b = ((lsmB :> IStorageBackend<int,int>).GetIterator()).GetAwaiter().GetResult() |> Seq.toArray |> Array.sort
        a = b

    [<Test>]
    [<Category("Stress")>]
    [<Explicit("Nightly stress profile")>]
    member _.``Order of updates does not affect final state (stress)``() =
        let property (updates: (int*int*int64) list) =
            let tmp1 = IO.Path.Combine(IO.Path.GetTempPath(), "dbsp_prop_order1_stress_" + string (Guid.NewGuid()))
            let tmp2 = IO.Path.Combine(IO.Path.GetTempPath(), "dbsp_prop_order2_stress_" + string (Guid.NewGuid()))
            IO.Directory.CreateDirectory(tmp1) |> ignore
            IO.Directory.CreateDirectory(tmp2) |> ignore
            let cfg1 = { DataPath = tmp1; MaxMemoryBytes = 10_000_000L; CompactionThreshold = 16; WriteBufferSize = 1024; BlockCacheSize = 1_000_000L; SpillThreshold = 0.8 }
            let cfg2 = { DataPath = tmp2; MaxMemoryBytes = 10_000_000L; CompactionThreshold = 16; WriteBufferSize = 1024; BlockCacheSize = 1_000_000L; SpillThreshold = 0.8 }
            let serKV = MessagePackSerializer<System.ValueTuple<int,int>>() :> ISerializer<System.ValueTuple<int,int>>
            let lsmA = LSMStorageBackend<int,int>(cfg1, serKV)
            let lsmB = LSMStorageBackend<int,int>(cfg2, serKV)
            (lsmA.StoreBatchWithFlush(updates, true)).GetAwaiter().GetResult()
            let shuffled = updates |> List.sortBy (fun _ -> Guid.NewGuid())
            (lsmB.StoreBatchWithFlush(shuffled, true)).GetAwaiter().GetResult()
            let a = ((lsmA :> IStorageBackend<int,int>).GetIterator()).GetAwaiter().GetResult() |> Seq.toArray |> Array.sort
            let b = ((lsmB :> IStorageBackend<int,int>).GetIterator()).GetAwaiter().GetResult() |> Seq.toArray |> Array.sort
            a = b
        Check.One(Config.Quick, Prop.forAll (Arb.fromGen StorageGenerators.updateGen) property)

    [<FsCheck.NUnit.Property(MaxTest = 30, Arbitrary = [| typeof<StorageArbsQuick> |])>]
    [<Category("Slow")>]
    member _.``Compaction is idempotent and preserves results``(updates: (int*int*int64) list) =
        let tmp = IO.Path.Combine(IO.Path.GetTempPath(), "dbsp_prop_compact_" + string (Guid.NewGuid()))
        IO.Directory.CreateDirectory(tmp) |> ignore
        let cfg = { DataPath = tmp; MaxMemoryBytes = 10_000_000L; CompactionThreshold = 64; WriteBufferSize = 16_384; BlockCacheSize = 1_000_000L; SpillThreshold = 0.8 }
        let serKV = MessagePackSerializer<System.ValueTuple<int,int>>() :> ISerializer<System.ValueTuple<int,int>>
        let lsm = LSMStorageBackend<int,int>(cfg, serKV)
        (lsm.StoreBatchWithFlush(updates, true)).GetAwaiter().GetResult()
        let before = ((lsm :> IStorageBackend<int,int>).GetIterator()).GetAwaiter().GetResult() |> Seq.toArray |> Array.sort
        (lsm.Compact()).GetAwaiter().GetResult()
        let after1 = ((lsm :> IStorageBackend<int,int>).GetIterator()).GetAwaiter().GetResult() |> Seq.toArray |> Array.sort
        (lsm.Compact()).GetAwaiter().GetResult()
        let after2 = ((lsm :> IStorageBackend<int,int>).GetIterator()).GetAwaiter().GetResult() |> Seq.toArray |> Array.sort
        before = after1 && after1 = after2

    [<Test>]
    [<Category("Stress")>]
    [<Explicit("Nightly stress profile")>]
    member _.``Compaction is idempotent and preserves results (stress)``() =
        let property (updates: (int*int*int64) list) =
            let tmp = IO.Path.Combine(IO.Path.GetTempPath(), "dbsp_prop_compact_stress_" + string (Guid.NewGuid()))
            IO.Directory.CreateDirectory(tmp) |> ignore
            let cfg = { DataPath = tmp; MaxMemoryBytes = 10_000_000L; CompactionThreshold = 8; WriteBufferSize = 1024; BlockCacheSize = 1_000_000L; SpillThreshold = 0.8 }
            let serKV = MessagePackSerializer<System.ValueTuple<int,int>>() :> ISerializer<System.ValueTuple<int,int>>
            let lsm = LSMStorageBackend<int,int>(cfg, serKV)
            (lsm.StoreBatchWithFlush(updates, true)).GetAwaiter().GetResult()
            let before = ((lsm :> IStorageBackend<int,int>).GetIterator()).GetAwaiter().GetResult() |> Seq.toArray |> Array.sort
            (lsm.Compact()).GetAwaiter().GetResult()
            let after1 = ((lsm :> IStorageBackend<int,int>).GetIterator()).GetAwaiter().GetResult() |> Seq.toArray |> Array.sort
            (lsm.Compact()).GetAwaiter().GetResult()
            let after2 = ((lsm :> IStorageBackend<int,int>).GetIterator()).GetAwaiter().GetResult() |> Seq.toArray |> Array.sort
            before = after1 && after1 = after2
        Check.One(Config.Quick, Prop.forAll (Arb.fromGen StorageGenerators.updateGen) property)

    [<FsCheck.NUnit.Property(MaxTest = 30, Arbitrary = [| typeof<StorageArbsQuick> |])>]
    [<Category("Slow")>]
    member _.``No zero-weight entries remain after updates and compaction``(updates: (int*int*int64) list) =
        let tmp = IO.Path.Combine(IO.Path.GetTempPath(), "dbsp_prop_nozero_" + string (Guid.NewGuid()))
        IO.Directory.CreateDirectory(tmp) |> ignore
        let cfg = { DataPath = tmp; MaxMemoryBytes = 10_000_000L; CompactionThreshold = 64; WriteBufferSize = 16_384; BlockCacheSize = 1_000_000L; SpillThreshold = 0.8 }
        let serKV = MessagePackSerializer<struct (int * int)>() :> ISerializer<struct (int * int)>
        let lsm = LSMStorageBackend<int,int>(cfg, serKV)
        (lsm.StoreBatchWithFlush(updates, true)).GetAwaiter().GetResult()
        (lsm.Compact()).GetAwaiter().GetResult()
        let seq = ((lsm :> IStorageBackend<int,int>).GetIterator()).GetAwaiter().GetResult()
        seq |> Seq.forall (fun (_,_,w) -> w <> 0L)

    [<Test>]
    [<Category("Stress")>]
    [<Explicit("Nightly stress profile")>]
    member _.``No zero-weight entries remain after updates and compaction (stress)``() =
        let property (updates: (int*int*int64) list) =
            let tmp = IO.Path.Combine(IO.Path.GetTempPath(), "dbsp_prop_nozero_stress_" + string (Guid.NewGuid()))
            IO.Directory.CreateDirectory(tmp) |> ignore
            let cfg = { DataPath = tmp; MaxMemoryBytes = 10_000_000L; CompactionThreshold = 8; WriteBufferSize = 1024; BlockCacheSize = 1_000_000L; SpillThreshold = 0.8 }
            let serKV = MessagePackSerializer<struct (int * int)>() :> ISerializer<struct (int * int)>
            let lsm = LSMStorageBackend<int,int>(cfg, serKV)
            (lsm.StoreBatchWithFlush(updates, true)).GetAwaiter().GetResult()
            (lsm.Compact()).GetAwaiter().GetResult()
            let seq = ((lsm :> IStorageBackend<int,int>).GetIterator()).GetAwaiter().GetResult()
            seq |> Seq.forall (fun (_,_,w) -> w <> 0L)
        Check.One(Config.Quick, Prop.forAll (Arb.fromGen StorageGenerators.updateGen) property)

    [<FsCheck.NUnit.Property(Arbitrary = [| typeof<TemporalArbs> |])>]
    member _.``TemporalSpine compaction preserves QueryAtTime results``(updatesByTime: (int64 * (int*int*int64) list) list) =
        let tmp = IO.Path.Combine(IO.Path.GetTempPath(), "dbsp_prop_temporal_" + string (Guid.NewGuid()))
        IO.Directory.CreateDirectory(tmp) |> ignore
        let cfg = { DataPath = tmp; MaxMemoryBytes = 10_000_000L; CompactionThreshold = 4; WriteBufferSize = 1024; BlockCacheSize = 1_000_000L; SpillThreshold = 0.8 }
        let serKV = MessagePackSerializer<struct (int * int)>() :> ISerializer<struct (int * int)>
        let spine = TemporalSpine<int,int>(cfg, serKV)
        for (t, ups) in updatesByTime do
            let arr = ups |> List.map (fun (k,v,w) -> struct (k,v,w)) |> List.toArray
            (spine.InsertBatch(t, arr)).GetAwaiter().GetResult()
        let qtime = if List.isEmpty updatesByTime then 0L else updatesByTime |> List.map fst |> List.max
        let before = (spine.QueryAtTime(qtime)).GetAwaiter().GetResult() |> Array.sort
        (spine.Compact(qtime)).GetAwaiter().GetResult()
        let after = (spine.QueryAtTime(qtime)).GetAwaiter().GetResult() |> Array.sort
        before = after
