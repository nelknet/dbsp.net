namespace DBSP.Tests.Performance

open BenchmarkDotNet.Attributes
open DBSP.Storage

[<MemoryDiagnoser>]
type StorageBenchmarks() =
    let tmp = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "dbsp_bench_" + string (System.Guid.NewGuid()))
    let config = { DataPath = tmp; MaxMemoryBytes = 100_000_000L; CompactionThreshold = 128; WriteBufferSize = 4096; BlockCacheSize = 10_000_000L; SpillThreshold = 0.8 }
    let serKV = MessagePackSerializer<struct (int * string)>() :> ISerializer<struct (int * string)>
    let lsm = LSMStorageBackend<int,string>(config, serKV) :> IStorageBackend<int,string>
    let data = [| for i in 1 .. 10_000 -> (i, "v", 1L) |]

    [<GlobalSetup>]
    member _.Setup() =
        System.IO.Directory.CreateDirectory(tmp) |> ignore
        ()

    [<Benchmark>]
    member _.WriteBatch() =
        lsm.StoreBatch(data) |> Async.AwaitTask |> Async.RunSynchronously

    [<Benchmark>]
    member _.IterateAll() =
        let it = lsm.GetIterator() |> Async.AwaitTask |> Async.RunSynchronously
        it |> Seq.length |> ignore

    [<Benchmark>]
    member _.SerializeCompressed() =
        let ser = SerializerFactory.CreateMessagePackCompressed<struct (int * string)>()
        ser.Serialize(struct (1, "v")) |> ignore

