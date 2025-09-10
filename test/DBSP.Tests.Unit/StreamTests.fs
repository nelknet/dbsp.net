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
        Assert.That(stream.IsEmpty, Is.True)
        Assert.That(stream.LatestTimestamp, Is.EqualTo None)

    [<Test>]
    member _.``singleton stream contains single value``() =
        let stream = Stream.singleton 100L 42
        Assert.That(stream.IsEmpty, Is.False)
        Assert.That(stream.GetValueAt(100L), Is.EqualTo (Some 42))
        Assert.That(stream.LatestTimestamp, Is.EqualTo (Some 100L))

    [<Test>]
    member _.``ofSeq creates stream correctly``() =
        let data = [(10L, "a"); (20L, "b"); (30L, "c")]
        let stream = Stream.ofSeq data
        
        Assert.That(stream.GetValueAt(10L), Is.EqualTo (Some "a"))
        Assert.That(stream.GetValueAt(20L), Is.EqualTo (Some "b"))
        Assert.That(stream.GetValueAt(30L), Is.EqualTo (Some "c"))
        Assert.That(stream.GetValueAt(40L), Is.EqualTo None)
        Assert.That(stream.LatestTimestamp, Is.EqualTo (Some 30L))

    [<Test>]
    member _.``toSeq returns ordered sequence``() =
        let data = [(30L, "c"); (10L, "a"); (20L, "b")]
        let stream = Stream.ofSeq data
        let result = Stream.toSeq stream |> Seq.toList
        
        let expected = [(10L, "a"); (20L, "b"); (30L, "c")]
        Assert.That(result, Is.EqualTo expected)

    [<Test>]
    member _.``timestamps property returns sorted timestamps``() =
        let data = [(30L, "c"); (10L, "a"); (20L, "b")]
        let stream = Stream.ofSeq data
        let timestamps = stream.Timestamps |> Seq.toList
        
        Assert.That(timestamps, Is.EqualTo [10L; 20L; 30L])

    [<Test>]
    member _.``combine works with custom combiner``() =
        let stream1 = Stream.ofSeq [(10L, 5); (20L, 3)]
        let stream2 = Stream.ofSeq [(10L, 2); (30L, 4)]
        let combined = Stream.combine stream1 stream2 (+)
        
        Assert.That(combined.GetValueAt(10L), Is.EqualTo (Some 7))
        Assert.That(combined.GetValueAt(20L), Is.EqualTo (Some 3))
        Assert.That(combined.GetValueAt(30L), Is.EqualTo (Some 4))
        Assert.That(combined.LatestTimestamp, Is.EqualTo (Some 30L))

    [<Test>]
    member _.``insertAt with integer addition``() =
        let stream = Stream.singleton 10L 5
        let updated = Stream.insertAt 10L 3 stream
        
        Assert.That(updated.GetValueAt(10L), Is.EqualTo (Some 8)) // 5 + 3

    [<Test>]
    member _.``integrateWith with string concatenation``() =
        let stream = Stream.ofSeq [(10L, "hello"); (20L, " world")]
        let integrated = Stream.integrateWith stream "" (+)
        
        Assert.That(integrated.GetValueAt(10L), Is.EqualTo (Some "hello")) 
        Assert.That(integrated.GetValueAt(20L), Is.EqualTo (Some "hello world"))

    [<Test>]
    member _.``delay shifts all timestamps``() =
        let stream = Stream.ofSeq [(10L, "a"); (20L, "b")]
        let delayed = Stream.delay 5L stream
        
        Assert.That(delayed.GetValueAt(15L), Is.EqualTo (Some "a")) // 10 + 5
        Assert.That(delayed.GetValueAt(25L), Is.EqualTo (Some "b")) // 20 + 5
        Assert.That(delayed.GetValueAt(10L), Is.EqualTo None)
        Assert.That(delayed.GetValueAt(20L), Is.EqualTo None)

    [<Test>]
    member _.``integrate computes cumulative sums correctly``() =
        let stream = Stream.ofSeq [(10L, 3); (20L, 2); (30L, 1)]
        let integrated = Stream.integrate stream  // Uses SRTP constraints for int
        
        Assert.That(integrated.GetValueAt(10L), Is.EqualTo (Some 3)) // 0 + 3
        Assert.That(integrated.GetValueAt(20L), Is.EqualTo (Some 5)) // 3 + 2  
        Assert.That(integrated.GetValueAt(30L), Is.EqualTo (Some 6)) // 5 + 1

    [<Test>]
    member _.``filterByTime works correctly``() =
        let stream = Stream.ofSeq [(10L, "a"); (20L, "b"); (30L, "c")]
        let filtered = Stream.filterByTime (fun t -> t >= 20L) stream
        
        Assert.That(filtered.GetValueAt(10L), Is.EqualTo None)
        Assert.That(filtered.GetValueAt(20L), Is.EqualTo (Some "b"))
        Assert.That(filtered.GetValueAt(30L), Is.EqualTo (Some "c"))

    [<Test>]
    member _.``filterByValue works correctly``() =
        let stream = Stream.ofSeq [(10L, "apple"); (20L, "banana"); (30L, "apricot")]
        let filtered = Stream.filterByValue (fun (s: string) -> s.StartsWith("a")) stream
        
        Assert.That(filtered.GetValueAt(10L), Is.EqualTo (Some "apple"))
        Assert.That(filtered.GetValueAt(20L), Is.EqualTo None) // "banana" filtered out
        Assert.That(filtered.GetValueAt(30L), Is.EqualTo (Some "apricot"))

    [<Test>]
    member _.``mapValues transforms values correctly``() =
        let stream = Stream.ofSeq [(10L, 5); (20L, 3); (30L, 2)]
        let mapped = Stream.mapValues (fun x -> x * 2) stream
        
        Assert.That(mapped.GetValueAt(10L), Is.EqualTo (Some 10))
        Assert.That(mapped.GetValueAt(20L), Is.EqualTo (Some 6))
        Assert.That(mapped.GetValueAt(30L), Is.EqualTo (Some 4))

[<TestFixture>]
type StreamPropertyTests() =

    [<FsCheck.NUnit.Property>]
    member _.``combine is commutative with commutative combiner``(data1: (int64 * int) list, data2: (int64 * int) list) =
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
                
                allTimestamps |> List.forall (fun t -> result1.GetValueAt(t) = result2.GetValueAt(t))

    [<FsCheck.NUnit.Property>]
    member _.``delay preserves stream structure``(offset: int64, data: (int64 * int) list) =
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
