namespace DBSP.Circuit

open System
open System.IO
open System.Text
open System.Collections.Concurrent

module internal Crc32 =
    let private table =
        let poly = 0xEDB88320u
        Array.init 256 (fun i ->
            let mutable crc = uint32 i
            for _ in 1 .. 8 do
                if (crc &&& 1u) = 1u then crc <- (crc >>> 1) ^^^ poly
                else crc <- crc >>> 1
            crc)
    let compute (bytes: byte[]) (offset:int) (count:int) =
        let mutable crc = 0xFFFFFFFFu
        for i in 0 .. count-1 do
            let idx = int ((crc ^^^ uint32 bytes[offset+i]) &&& 0xFFu)
            crc <- table[idx] ^^^ (crc >>> 8)
        crc ^^^ 0xFFFFFFFFu

type WalEvent =
    | BeginEpoch of int64
    | EndEpoch of int64
    | CheckpointCreated of epoch:int64 * name:string
    | RestoredFromCheckpoint of epoch:int64 * name:string

type WriteAheadLog(logPath: string) =
    let filePath = Path.Combine(logPath, "wal.bin")
    do Directory.CreateDirectory(logPath) |> ignore

    // Binary layout per record:
    // [u32 payloadLen][payload bytes][u32 crc]
    // payload: [u8 type][i64 epoch][u16 nameLen][name bytes]
    // File header (once): ASCII "DBSPWAL1" (8 bytes)

    let ensureHeader (fs: FileStream) =
        if fs.Length = 0L then
            let header = Encoding.ASCII.GetBytes("DBSPWAL1")
            fs.Write(header, 0, header.Length)
            fs.Flush()

    member _.Append(ev: WalEvent) =
        use fs = new FileStream(filePath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read)
        fs.Seek(0L, SeekOrigin.End) |> ignore
        ensureHeader fs
        use ms = new MemoryStream()
        use pw = new BinaryWriter(ms, Encoding.UTF8, true)
        match ev with
        | BeginEpoch e -> pw.Write(byte 1); pw.Write(e); pw.Write(uint16 0)
        | EndEpoch e -> pw.Write(byte 2); pw.Write(e); pw.Write(uint16 0)
        | CheckpointCreated (e, n) ->
            let nb = Encoding.UTF8.GetBytes(n)
            pw.Write(byte 3); pw.Write(e); pw.Write(uint16 nb.Length); pw.Write(nb)
        | RestoredFromCheckpoint (e, n) ->
            let nb = Encoding.UTF8.GetBytes(n)
            pw.Write(byte 4); pw.Write(e); pw.Write(uint16 nb.Length); pw.Write(nb)
        pw.Flush()
        let payload = ms.ToArray()
        let crc = Crc32.compute payload 0 payload.Length
        use bw = new BinaryWriter(fs, Encoding.UTF8, true)
        bw.Write(uint32 payload.Length)
        bw.Write(payload)
        bw.Write(crc)
        bw.Flush()

    member _.GetLastCommittedEpoch() =
        if not (File.Exists(filePath)) then 0L else
        use fs = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)
        use br = new BinaryReader(fs, Encoding.UTF8, true)
        let mutable lastEnd = 0L
        let mutable ok = true
        try
            if fs.Length < 8L then 0L else
            let hdr = br.ReadBytes(8)
            if Encoding.ASCII.GetString(hdr) <> "DBSPWAL1" then 0L else
            while ok && fs.Position + 8L <= fs.Length do
                let payloadLen = int (br.ReadUInt32())
                if payloadLen < 0 || fs.Position + int64 payloadLen + 4L > fs.Length then ok <- false
                else
                    let payload = br.ReadBytes(payloadLen)
                    let crcRead = br.ReadUInt32()
                    let crcCalc = Crc32.compute payload 0 payloadLen
                    if crcRead <> crcCalc then ok <- false
                    else
                        use ms = new MemoryStream(payload)
                        use pr = new BinaryReader(ms, Encoding.UTF8, true)
                        let rtype = pr.ReadByte()
                        let epoch = pr.ReadInt64()
                        let nameLen = int (pr.ReadUInt16())
                        if nameLen > 0 then ignore (pr.ReadBytes(nameLen))
                        if rtype = 2uy then lastEnd <- epoch
            lastEnd
        with _ -> lastEnd

    member _.TruncateToLastGoodRecord() =
        if not (File.Exists(filePath)) then () else
        use fs = new FileStream(filePath, FileMode.Open, FileAccess.ReadWrite, FileShare.None)
        use br = new BinaryReader(fs, Encoding.UTF8, true)
        // Validate header
        if fs.Length < 8L then fs.SetLength(0L) else
            let hdr = br.ReadBytes(8)
            if Encoding.ASCII.GetString(hdr) <> "DBSPWAL1" then fs.SetLength(0L) else
                let mutable ok = true
                let mutable lastGoodPos = fs.Position
                while ok && fs.Position + 8L <= fs.Length do
                    let pos = fs.Position
                    let payloadLen = int (br.ReadUInt32())
                    if payloadLen < 0 || fs.Position + int64 payloadLen + 4L > fs.Length then ok <- false
                    else
                        let payload = br.ReadBytes(payloadLen)
                        let crcRead = br.ReadUInt32()
                        let crcCalc = Crc32.compute payload 0 payloadLen
                        if crcRead <> crcCalc then ok <- false else lastGoodPos <- fs.Position
                fs.SetLength(lastGoodPos)

    member _.GetLatestCheckpoint() : (int64 * string) option =
        if not (File.Exists(filePath)) then None else
        use fs = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)
        use br = new BinaryReader(fs, Encoding.UTF8, true)
        try
            if fs.Length < 8L then None else
            let hdr = br.ReadBytes(8)
            if Encoding.ASCII.GetString(hdr) <> "DBSPWAL1" then None else
            let mutable last : (int64 * string) option = None
            let mutable ok = true
            while ok && fs.Position + 8L <= fs.Length do
                let payloadLen = int (br.ReadUInt32())
                if payloadLen < 0 || fs.Position + int64 payloadLen + 4L > fs.Length then ok <- false
                else
                    let payload = br.ReadBytes(payloadLen)
                    let crcRead = br.ReadUInt32()
                    let crcCalc = Crc32.compute payload 0 payloadLen
                    if crcRead <> crcCalc then ok <- false
                    else
                        use ms = new MemoryStream(payload)
                        use pr = new BinaryReader(ms, Encoding.UTF8, true)
                        let rtype = pr.ReadByte()
                        let epoch = pr.ReadInt64()
                        let nameLen = int (pr.ReadUInt16())
                        let name = if nameLen > 0 then Encoding.UTF8.GetString(pr.ReadBytes(nameLen)) else ""
                        if rtype = 3uy then last <- Some (epoch, name)
            last
        with _ -> None
