module DBSP.Tests.Unit.WALCorruptionTests

open NUnit.Framework
open DBSP.Circuit
open System.IO

[<Test>]
let ``WAL tolerates partial trailing record`` () =
    let basePath = Path.Combine(System.IO.Path.GetTempPath(), "dbsp_wal_corrupt")
    if Directory.Exists(basePath) then Directory.Delete(basePath, true)
    let wal = new WriteAheadLog(basePath)
    wal.Append(BeginEpoch 1L)
    wal.Append(EndEpoch 1L)
    wal.Append(BeginEpoch 2L)
    // Manually corrupt by truncating a few bytes at end
    let f = Path.Combine(basePath, "wal.bin")
    use fs = new FileStream(f, FileMode.Open, FileAccess.ReadWrite, FileShare.Read)
    if fs.Length > 4L then fs.SetLength(fs.Length - 3L)
    Assert.That(wal.GetLastCommittedEpoch(), Is.EqualTo 1L)

