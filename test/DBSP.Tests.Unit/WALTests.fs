module DBSP.Tests.Unit.WALTests

open NUnit.Framework
open DBSP.Circuit
open System.IO

[<Test>]
let ``WAL records begin/end and reports last committed epoch`` () =
    let path = Path.Combine(System.IO.Path.GetTempPath(), "dbsp_wal_test")
    if Directory.Exists(path) then Directory.Delete(path, true)
    let wal = new WriteAheadLog(path)
    wal.Append(BeginEpoch 1L)
    wal.Append(EndEpoch 1L)
    wal.Append(BeginEpoch 2L)
    Assert.That(wal.GetLastCommittedEpoch(), Is.EqualTo 1L)
    wal.Append(EndEpoch 2L)
    Assert.That(wal.GetLastCommittedEpoch(), Is.EqualTo 2L)

