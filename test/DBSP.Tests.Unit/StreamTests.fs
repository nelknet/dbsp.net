/// Unit tests for DBSP.Core.Stream module
module StreamTests

open NUnit.Framework
open FsCheck
open DBSP.Core.Stream

[<TestFixture>]
type StreamBasicOperationTests() =

    [<Test>]
    member _.``empty stream is indeed empty``() =
        let stream = Stream.empty<int>
        Assert.IsTrue(stream.IsEmpty)
        Assert.AreEqual(None, stream.LatestTimestamp)

    [<Test>]
    member _.``singleton stream contains single value``() =
        let stream = Stream.singleton 100L 42
        Assert.IsFalse(stream.IsEmpty)
        Assert.AreEqual(Some 42, stream.GetValueAt(100L))
        Assert.AreEqual(Some 100L, stream.LatestTimestamp)

    [<Test>]
    member _.``ofSeq creates stream correctly``() =
        let data = [(10L, "a"); (20L, "b"); (30L, "c")]
        let stream = Stream.ofSeq data
        
        Assert.AreEqual(Some "a", stream.GetValueAt(10L))
        Assert.AreEqual(Some "b", stream.GetValueAt(20L))
        Assert.AreEqual(Some "c", stream.GetValueAt(30L))
        Assert.AreEqual(None, stream.GetValueAt(40L))
        Assert.AreEqual(Some 30L, stream.LatestTimestamp)

    [<Test>]
    member _.``toSeq returns ordered sequence``() =
        let data = [(30L, "c"); (10L, "a"); (20L, "b")]
        let stream = Stream.ofSeq data
        let result = Stream.toSeq stream |> Seq.toList
        
        let expected = [(10L, "a"); (20L, "b"); (30L, "c")]
        Assert.AreEqual(expected, result)

    [<Test>]
    member _.``timestamps property returns sorted timestamps``() =
        let data = [(30L, "c"); (10L, "a"); (20L, "b")]
        let stream = Stream.ofSeq data
        let timestamps = stream.Timestamps |> Seq.toList
        
        Assert.AreEqual([10L; 20L; 30L], timestamps)

    [<Test>]
    member _.``combine works with custom combiner``() =
        let stream1 = Stream.ofSeq [(10L, 5); (20L, 3)]
        let stream2 = Stream.ofSeq [(10L, 2); (30L, 4)]
        let combined = Stream.combine stream1 stream2 (+)
        
        Assert.AreEqual(Some 7, combined.GetValueAt(10L))  // 5 + 2
        Assert.AreEqual(Some 3, combined.GetValueAt(20L))  // 3 + 0 (no value at 20L in stream2)
        Assert.AreEqual(Some 4, combined.GetValueAt(30L))  // 0 + 4
        Assert.AreEqual(Some 30L, combined.LatestTimestamp)

    [<Test>]
    member _.``insertAt with integer addition``() =
        let stream = Stream.singleton 10L 5
        let updated = Stream.insertAt 10L 3 stream
        
        Assert.AreEqual(Some 8, updated.GetValueAt(10L)) // 5 + 3

    [<Test>]
    member _.``integrateWith with string concatenation``() =
        let stream = Stream.ofSeq [(10L, "hello"); (20L, " world")]
        let integrated = Stream.integrateWith stream "" (+)
        
        Assert.AreEqual(Some "hello", integrated.GetValueAt(10L)) 
        Assert.AreEqual(Some "hello world", integrated.GetValueAt(20L))

    [<Test>]
    member _.``delay shifts all timestamps``() =
        let stream = Stream.ofSeq [(10L, "a"); (20L, "b")]
        let delayed = Stream.delay 5L stream
        
        Assert.AreEqual(Some "a", delayed.GetValueAt(15L)) // 10 + 5
        Assert.AreEqual(Some "b", delayed.GetValueAt(25L)) // 20 + 5
        Assert.AreEqual(None, delayed.GetValueAt(10L))
        Assert.AreEqual(None, delayed.GetValueAt(20L))

    [<Test>]
    member _.``integrate computes cumulative sums correctly``() =
        let stream = Stream.ofSeq [(10L, 3); (20L, 2); (30L, 1)]
        let integrated = Stream.integrate stream  // Uses SRTP constraints for int
        
        Assert.AreEqual(Some 3, integrated.GetValueAt(10L)) // 0 + 3
        Assert.AreEqual(Some 5, integrated.GetValueAt(20L)) // 3 + 2  
        Assert.AreEqual(Some 6, integrated.GetValueAt(30L)) // 5 + 1

    [<Test>]
    member _.``filterByTime works correctly``() =
        let stream = Stream.ofSeq [(10L, "a"); (20L, "b"); (30L, "c")]
        let filtered = Stream.filterByTime (fun t -> t >= 20L) stream
        
        Assert.AreEqual(None, filtered.GetValueAt(10L))
        Assert.AreEqual(Some "b", filtered.GetValueAt(20L))
        Assert.AreEqual(Some "c", filtered.GetValueAt(30L))

    [<Test>]
    member _.``filterByValue works correctly``() =
        let stream = Stream.ofSeq [(10L, "apple"); (20L, "banana"); (30L, "apricot")]
        let filtered = Stream.filterByValue (fun (s: string) -> s.StartsWith("a")) stream
        
        Assert.AreEqual(Some "apple", filtered.GetValueAt(10L))
        Assert.AreEqual(None, filtered.GetValueAt(20L)) // "banana" filtered out
        Assert.AreEqual(Some "apricot", filtered.GetValueAt(30L))

    [<Test>]
    member _.``mapValues transforms values correctly``() =
        let stream = Stream.ofSeq [(10L, 5); (20L, 3); (30L, 2)]
        let mapped = Stream.mapValues (fun x -> x * 2) stream
        
        Assert.AreEqual(Some 10, mapped.GetValueAt(10L))
        Assert.AreEqual(Some 6, mapped.GetValueAt(20L))
        Assert.AreEqual(Some 4, mapped.GetValueAt(30L))

[<TestFixture>]
type StreamPropertyTests() =

    [<Test>]
    member _.``combine is commutative with commutative combiner``() =
        let property (data1: (int64 * int) list) (data2: (int64 * int) list) =
            // Filter to positive timestamps to avoid issues
            let filteredData1 = data1 |> List.filter (fun (t, _) -> t >= 0L)
            let filteredData2 = data2 |> List.filter (fun (t, _) -> t >= 0L)
            
            if List.isEmpty filteredData1 || List.isEmpty filteredData2 then
                true // Skip empty cases
            else
                let stream1 = Stream.ofSeq filteredData1
                let stream2 = Stream.ofSeq filteredData2
                let result1 = Stream.combine stream1 stream2 (+)
                let result2 = Stream.combine stream2 stream1 (+)
                
                // Check that values are equal at all timestamps
                let allTimestamps = 
                    [result1.Timestamps; result2.Timestamps]
                    |> Seq.collect id
                    |> Seq.distinct
                    |> Seq.toList
                
                allTimestamps |> List.forall (fun t ->
                    result1.GetValueAt(t) = result2.GetValueAt(t))
        
        Check.QuickThrowOnFailure property

    [<Test>]
    member _.``delay preserves stream structure``() =
        let property (offset: int64) (data: (int64 * int) list) =
            // Use positive offset and timestamps, and ensure unique timestamps
            let safeOffset = abs offset + 1L
            let uniqueData = 
                data 
                |> List.map (fun (t, v) -> (abs t, v))
                |> List.groupBy fst
                |> List.map (fun (t, pairs) -> (t, pairs |> List.last |> snd)) // Take last value for duplicate timestamps
            
            if List.isEmpty uniqueData then
                true
            else
                let stream = Stream.ofSeq uniqueData
                let delayed = Stream.delay safeOffset stream
                
                // Check that all values are preserved, just shifted
                uniqueData |> List.forall (fun (originalTime, value) ->
                    let newTime = originalTime + safeOffset
                    delayed.GetValueAt(newTime) = Some value)
        
        Check.QuickThrowOnFailure property