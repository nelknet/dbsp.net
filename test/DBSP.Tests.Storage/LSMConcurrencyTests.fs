module DBSP.Tests.Storage.LSMConcurrencyTests

open System
open System.Threading.Tasks
open NUnit.Framework
open DBSP.Storage

let mkConfig tmp =
    { DataPath = tmp
      MaxMemoryBytes = 10_000_000L
      CompactionThreshold = 16
      WriteBufferSize = 1024
      BlockCacheSize = 1_000_000L
      SpillThreshold = 0.8 }

[<Test>]
let ``Concurrent writes allow consistent iteration and final state`` () = task {
    let tmp = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "dbsp_lsm_conc_" + string (Guid.NewGuid()))
    System.IO.Directory.CreateDirectory(tmp) |> ignore
    let config = mkConfig tmp
    let ser = MessagePackSerializer<struct (int * string)>() :> ISerializer<struct (int * string)>
    let storage = LSMStorageBackend<int,string>(config, ser) :> IStorageBackend<int,string>

    // 4 concurrent writers covering disjoint key ranges [1..1000]
    let writer (w:int) = task {
        let startKey = w * 250 + 1
        let endKey = (w + 1) * 250
        // Write in small chunks to increase interleaving
        let mutable s = startKey
        while s <= endKey do
            let e = min endKey (s + 49)
            let batchPos = [ for i in s .. e -> (i, "V", 1L) ]
            let batchNeg = [ for i in s .. e do if i % 10 = 0 then yield (i, "V", -1L) ]
            do! storage.StoreBatch(Seq.append batchPos batchNeg)
            do! Task.Delay(1)
            s <- e + 1
    }

    // Readers repeatedly iterate and verify monotonic key order and no zero weights
    let reader = task {
        for _ in 1 .. 5 do
            let! it = storage.GetIterator()
            let arr = it |> Seq.toArray
            let isSorted =
                arr
                |> Array.pairwise
                |> Array.forall (fun ((k1,_,_), (k2,_,_)) -> k1 <= k2)
            Assert.That(isSorted, Is.True)
            Assert.That(arr |> Array.exists (fun (_,_,w) -> w = 0L), Is.False)
            do! Task.Delay(5)
    }

    let writers = [| for w in 0 .. 3 -> writer w |]
    let readers = [| reader; reader |]
    let! _ = Task.WhenAll(Array.append writers readers)

    // Final state: 1000 keys, every 10th was cancelled => 900 remaining
    let! finalIt = storage.GetIterator()
    let finalArr = finalIt |> Seq.toArray
    Assert.That(finalArr.Length, Is.EqualTo 900)
    Assert.That(finalArr |> Array.exists (fun (k,_,_) -> k % 10 = 0), Is.False)
}
