namespace DBSP.Circuit

open System
open System.IO
open System.Text
open System.Threading.Tasks
open DBSP.Operators.Interfaces

type CheckpointConfig = {
    Path: string
    Name: string
}

type private OperatorStateMeta = {
    NodeId: int64
    FileName: string
    Size: int64
}

// Binary manifest (header "DBSPCP1"):
// i64 circuitId, i64 epoch, u16 nameLen, name bytes, u32 opCount, then for each:
// i64 nodeId, u16 fileNameLen, fileName bytes, i64 size

type CircuitCheckpointManager(circuit: CircuitDefinition, wal: WriteAheadLog, circuitId: int64) =
    // Cache SerializeState/DeserializeState MethodInfos per operator type to reduce reflection overhead
    let methodCache = System.Collections.Concurrent.ConcurrentDictionary<Type, (Reflection.MethodInfo * Reflection.MethodInfo) option>()
    let tryGetStateMethods (o: obj) =
        let t = o.GetType()
        methodCache.GetOrAdd(t, fun t' ->
            let iface = t'.GetInterfaces() |> Array.tryFind (fun i -> i.IsGenericType && i.GetGenericTypeDefinition() = typedefof<IStatefulOperator<_>>)
            match iface with
            | None -> None
            | Some _ ->
                let ser = t'.GetMethod("SerializeState")
                let de = t'.GetMethod("DeserializeState")
                if isNull ser || isNull de then None else Some (ser, de))

    member _.CreateCheckpointAsync(epoch: int64, cfg: CheckpointConfig) = task {
        Directory.CreateDirectory(cfg.Path) |> ignore
        let cpDir = Path.Combine(cfg.Path, $"cp_{epoch}")
        Directory.CreateDirectory(cpDir) |> ignore
        let! metas =
            circuit.Operators
            |> Seq.map (fun kvp -> task {
                let nodeId = kvp.Key.Id
                let o = kvp.Value
                match tryGetStateMethods o with
                | Some (ser, _de) ->
                    let! bytes = ser.Invoke(o, [||]) :?> Task<byte[]>
                    let fileName = $"op_{nodeId}.bin"
                    let path = Path.Combine(cpDir, fileName)
                    do! File.WriteAllBytesAsync(path, bytes)
                    return Some { NodeId = nodeId; FileName = fileName; Size = int64 bytes.Length }
                | None -> return None })
            |> Task.WhenAll
        let ops = metas |> Array.choose id
        let manifestPath = Path.Combine(cpDir, "manifest.bin")
        use ms = new MemoryStream()
        use pw = new BinaryWriter(ms, Encoding.UTF8, true)
        // payload
        pw.Write(circuitId)
        pw.Write(epoch)
        let nameBytes = Encoding.UTF8.GetBytes(cfg.Name)
        pw.Write(uint16 nameBytes.Length)
        pw.Write(nameBytes)
        pw.Write(uint32 ops.Length)
        for op in ops do
            pw.Write(op.NodeId)
            let fnb = Encoding.UTF8.GetBytes(op.FileName)
            pw.Write(uint16 fnb.Length)
            pw.Write(fnb)
            pw.Write(op.Size)
        pw.Flush()
        let payload = ms.ToArray()
        let crc = Crc32.compute payload 0 payload.Length
        use fs = new FileStream(manifestPath, FileMode.Create, FileAccess.Write, FileShare.None)
        use bw = new BinaryWriter(fs, Encoding.UTF8, true)
        bw.Write(Encoding.ASCII.GetBytes("DBSPCP2"))
        bw.Write(uint32 payload.Length)
        bw.Write(payload)
        bw.Write(crc)
        bw.Flush()
        wal.Append(WalEvent.CheckpointCreated(epoch, cfg.Name))
        return cpDir
    }

    member _.RestoreFromCheckpointAsync(epoch: int64, cfg: CheckpointConfig) = task {
        let cpDir = Path.Combine(cfg.Path, $"cp_{epoch}")
        let manifestPath = Path.Combine(cpDir, "manifest.bin")
        if not (File.Exists(manifestPath)) then
            return Error (sprintf "Checkpoint manifest not found: %s" manifestPath)
        else
            use fs = new FileStream(manifestPath, FileMode.Open, FileAccess.Read, FileShare.Read)
            use br = new BinaryReader(fs, Encoding.UTF8, true)
            let header = br.ReadBytes(7)
            let hdr = Encoding.ASCII.GetString(header)
            if hdr = "DBSPCP2" then
                let payloadLen = int (br.ReadUInt32())
                if payloadLen <= 0 || fs.Position + int64 payloadLen + 4L > fs.Length then return Error "Corrupt manifest length" else
                let payload = br.ReadBytes(payloadLen)
                let crcRead = br.ReadUInt32()
                let crcCalc = Crc32.compute payload 0 payloadLen
                if crcRead <> crcCalc then return Error "Corrupt manifest checksum" else
                use ms = new MemoryStream(payload)
                use pr = new BinaryReader(ms, Encoding.UTF8, true)
                let _cid = pr.ReadInt64() |> ignore
                let _epoch = pr.ReadInt64() |> ignore
                let nameLen = int (pr.ReadUInt16())
                let _name = if nameLen > 0 then Encoding.UTF8.GetString(pr.ReadBytes(nameLen)) else ""
                let opCount = int (pr.ReadUInt32())
                for _ in 1 .. opCount do
                    let nodeId = pr.ReadInt64()
                    let fnl = int (pr.ReadUInt16())
                    let fn = Encoding.UTF8.GetString(pr.ReadBytes(fnl))
                    let _size = pr.ReadInt64() |> ignore
                    let filePath = Path.Combine(cpDir, fn)
                    if File.Exists(filePath) then
                        match circuit.Operators |> Map.tryFind { Id = nodeId } with
                        | Some o ->
                            match tryGetStateMethods o with
                            | Some (_ser, de) ->
                                let bytes = File.ReadAllBytes(filePath)
                                let! _ = de.Invoke(o, [| box bytes |]) :?> Task
                                ()
                            | None -> ()
                        | None -> ()
                wal.Append(WalEvent.RestoredFromCheckpoint(epoch, cfg.Name))
                return Ok ()
            elif hdr = "DBSPCP1" then
                // Backward-compatible path (no checksum)
                let _cid = br.ReadInt64() |> ignore
                let _epoch = br.ReadInt64() |> ignore
                let nameLen = int (br.ReadUInt16())
                let _name = if nameLen > 0 then Encoding.UTF8.GetString(br.ReadBytes(nameLen)) else ""
                let opCount = int (br.ReadUInt32())
                for _ in 1 .. opCount do
                    let nodeId = br.ReadInt64()
                    let fnl = int (br.ReadUInt16())
                    let fn = Encoding.UTF8.GetString(br.ReadBytes(fnl))
                    let _size = br.ReadInt64() |> ignore
                    let filePath = Path.Combine(cpDir, fn)
                    if File.Exists(filePath) then
                        match circuit.Operators |> Map.tryFind { Id = nodeId } with
                        | Some o ->
                            match tryGetStateMethods o with
                            | Some (_ser, de) ->
                                let bytes = File.ReadAllBytes(filePath)
                                let! _ = de.Invoke(o, [| box bytes |]) :?> Task
                                ()
                            | None -> ()
                        | None -> ()
                wal.Append(WalEvent.RestoredFromCheckpoint(epoch, cfg.Name))
                return Ok ()
            else
                return Error "Invalid manifest header"
    }
