module DBSP.Tests.Unit.CheckpointManifestCrcTests

open NUnit.Framework
open System.IO
open DBSP.Circuit
open DBSP.Operators.Interfaces
open System.Threading.Tasks

type SmallStateOp() =
    inherit BaseOperator("Small")
    let mutable s = 42
    interface IStatefulOperator<int> with
        member _.GetState() = s
        member _.SetState(v) = s <- v
        member _.SerializeState() = task { return System.BitConverter.GetBytes(s) }
        member _.DeserializeState(bytes: byte[]) = task { s <- System.BitConverter.ToInt32(bytes, 0) }

[<Test>]
let ``Manifest CRC detects corruption`` () =
    let (c, _op) = RootCircuit.Build(fun b ->
        let op = SmallStateOp()
        b.AddOperator(op, { Name = "Small"; TypeInfo = "Small"; Location = None }) |> ignore
        op)
    let basePath = Path.Combine(Path.GetTempPath(), "dbsp_ckpt_crc")
    if Directory.Exists(basePath) then Directory.Delete(basePath, true)
    let cfg = { RuntimeConfig.Default with EnableCheckpointing = true; StoragePath = Some basePath }
    let rt = CircuitRuntimeModule.create c cfg
    match rt.CreateCheckpointAsync("crc").Result with | Ok () -> () | Error e -> Assert.Fail(e)
    let cpDir = Path.Combine(basePath, "checkpoints", sprintf "cp_%d" rt.CurrentEpoch)
    let man = Path.Combine(cpDir, "manifest.bin")
    // Corrupt last byte
    use fs = new FileStream(man, FileMode.Open, FileAccess.ReadWrite, FileShare.None)
    fs.Seek(-1L, SeekOrigin.End) |> ignore
    let b = fs.ReadByte()
    fs.Seek(-1L, SeekOrigin.End) |> ignore
    fs.WriteByte(byte (b ^^^ 0xFF))
    fs.Flush()
    // Attempt restore should error
    match rt.RestoreCheckpointAsync(rt.CurrentEpoch, "crc").Result with
    | Ok () -> Assert.Fail("Expected CRC failure")
    | Error _ -> ()

