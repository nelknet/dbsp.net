namespace DBSP.Tests.Performance

open BenchmarkDotNet.Attributes
open DBSP.Storage
open BenchmarkDotNet.Jobs

[<MemoryDiagnoser>]
type StorageBenchmarks() =
    let tmp = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "dbsp_bench_" + string (System.Guid.NewGuid()))
    let config = { DataPath = tmp; MaxMemoryBytes = 512_000_000L; CompactionThreshold = 256; WriteBufferSize = 1 <<< 15; BlockCacheSize = 32_000_000L; SpillThreshold = 0.8 }
    let serKV = MessagePackSerializer<struct (int * string)>() :> ISerializer<struct (int * string)>
    let lsm = LSMStorageBackend<int,string>(config, serKV) :> IStorageBackend<int,string>
    let mutable data : (int*string*int64) array = [||]

    [<Params(10000)>]
    member val BatchSize = 0 with get, set

    [<GlobalSetup>]
    member this.Setup() =
        System.IO.Directory.CreateDirectory(tmp) |> ignore
        data <- [| for i in 1 .. this.BatchSize -> (i, "v", 1L) |]

    [<Benchmark(Description="WriteBatch (no flush)")>]
    member _.WriteBatch() =
        lsm.StoreBatch(data) |> Async.AwaitTask |> Async.RunSynchronously

    [<Benchmark(Description="WriteBatchWithFlush (force memtable forward)")>]
    member _.WriteBatchWithFlush() =
        (lsm :?> LSMStorageBackend<int,string>).StoreBatchWithFlush(data, true) |> Async.AwaitTask |> Async.RunSynchronously

    [<Benchmark(Description="IterateAll (full scan)")>]
    member _.IterateAll() =
        let it = lsm.GetIterator() |> Async.AwaitTask |> Async.RunSynchronously
        it |> Seq.length |> ignore

    [<Benchmark(Description="SerializeCompressed (MessagePack+Zstd)")>]
    member _.SerializeCompressed() =
        let ser = SerializerFactory.CreateMessagePackCompressed<struct (int * string)>()
        ser.Serialize(struct (1, "v")) |> ignore

    [<Benchmark(Description="Compact (explicit)")>]
    member _.Compact() =
        lsm.Compact() |> Async.AwaitTask |> Async.RunSynchronously
