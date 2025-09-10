/// Unit tests for DBSP.Core.IndexedZSet module
module IndexedZSetTests

open NUnit.Framework
open FsCheck
open DBSP.Core.ZSet
open DBSP.Core.IndexedZSet

[<TestFixture>]
type IndexedZSetBasicTests() =

    [<Test>]
    member _.``empty IndexedZSet is indeed empty``() =
        let indexed = IndexedZSet.empty<int, string>
        Assert.That(indexed.IsEmpty, Is.True)
        Assert.That(indexed.TotalCount, Is.EqualTo 0)

    [<Test>]
    member _.``ofSeq creates IndexedZSet correctly``() =
        let data = [(1, "a", 3); (1, "b", 2); (2, "c", -1)]
        let indexed = IndexedZSet.ofSeq data
        
        let zset1 = indexed.GetZSet(1)
        let zset2 = indexed.GetZSet(2)
        
        Assert.That(zset1.GetWeight("a"), Is.EqualTo 3)
        Assert.That(zset1.GetWeight("b"), Is.EqualTo 2)
        Assert.That(zset2.GetWeight("c"), Is.EqualTo -1)

    [<Test>]
    member _.``groupBy creates correct index``() =
        let pairs = [("apple", 5); ("banana", 3); ("apricot", 2); ("blueberry", 4)]
        let zset = ZSet.ofList pairs
        let indexed = IndexedZSet.groupBy (fun (name: string) -> name.[0]) zset
        
        let aGroup = indexed.GetZSet('a')
        let bGroup = indexed.GetZSet('b')
        
        Assert.That(aGroup.GetWeight("apple"), Is.EqualTo 5)
        Assert.That(aGroup.GetWeight("apricot"), Is.EqualTo 2)
        Assert.That(bGroup.GetWeight("banana"), Is.EqualTo 3)
        Assert.That(bGroup.GetWeight("blueberry"), Is.EqualTo 4)

    [<Test>]
    member _.``fromZSet and toZSet are inverses``() =
        let pairs = [((1, "a"), 3); ((1, "b"), 2); ((2, "c"), -1)]
        let zset = ZSet.ofList pairs
        let indexed = IndexedZSet.fromZSet zset
        let backToZSet = IndexedZSet.toZSet indexed
        
        // Check that we get back equivalent ZSet
        pairs |> List.iter (fun ((k, v), w) -> Assert.That(backToZSet.GetWeight((k, v)), Is.EqualTo w))

    [<Test>]
    member _.``join produces cartesian product for matching keys``() =
        let left = IndexedZSet.ofSeq [(1, "a", 2); (2, "b", 1)]
        let right = IndexedZSet.ofSeq [(1, 10, 3); (1, 20, 1); (3, 30, 2)]
        let joined = IndexedZSet.join left right
        
        // For key 1: ("a", 2) × (10, 3) and ("a", 2) × (20, 1) = weights 6 and 2
        let result1 = joined.GetZSet(1)
        Assert.That(result1.GetWeight(("a", 10)), Is.EqualTo 6) // 2 * 3
        Assert.That(result1.GetWeight(("a", 20)), Is.EqualTo 2) // 2 * 1
        
        // Key 2 has no match in right, so no results
        let result2 = joined.GetZSet(2)
        Assert.That(result2.IsEmpty, Is.True)

    [<Test>]
    member _.``filterByKey works correctly``() =
        let indexed = IndexedZSet.ofSeq [(1, "a", 3); (2, "b", 2); (3, "c", 1)]
        let filtered = IndexedZSet.filterByKey (fun k -> k % 2 = 1) indexed
        
        Assert.That(filtered.GetZSet(1).IsEmpty, Is.False)
        Assert.That(filtered.GetZSet(2).IsEmpty, Is.True)
        Assert.That(filtered.GetZSet(3).IsEmpty, Is.False)

    [<Test>]
    member _.``filterByValue works correctly``() =
        let indexed = IndexedZSet.ofSeq [(1, "apple", 3); (1, "ant", 2); (2, "banana", 1)]
        let filtered = IndexedZSet.filterByValue (fun (s: string) -> s.StartsWith("a")) indexed
        
        let zset1 = filtered.GetZSet(1)
        Assert.That(zset1.GetWeight("apple"), Is.EqualTo 3)
        Assert.That(zset1.GetWeight("ant"), Is.EqualTo 2)
        Assert.That(zset1.GetWeight("other"), Is.EqualTo 0)
        
        let zset2 = filtered.GetZSet(2)
        Assert.That(zset2.IsEmpty, Is.True) // "banana" filtered out

[<TestFixture>]
type IndexedZSetAlgebraicTests() =

    [<Test>]
    member _.``IndexedZSet addition combines ZSets correctly``() =
        let indexed1 = IndexedZSet.ofSeq [(1, "a", 3); (2, "b", 2)]
        let indexed2 = IndexedZSet.ofSeq [(1, "a", 2); (1, "c", 1); (3, "d", -1)]
        let result = IndexedZSet.add indexed1 indexed2
        
        let zset1 = result.GetZSet(1)
        Assert.That(zset1.GetWeight("a"), Is.EqualTo 5) // 3 + 2
        Assert.That(zset1.GetWeight("c"), Is.EqualTo 1) // 0 + 1
        
        let zset2 = result.GetZSet(2)
        Assert.That(zset2.GetWeight("b"), Is.EqualTo 2) // 2 + 0
        
        let zset3 = result.GetZSet(3)
        Assert.That(zset3.GetWeight("d"), Is.EqualTo -1) // 0 + (-1)

    [<Test>]
    member _.``IndexedZSet negation flips all weights``() =
        let indexed = IndexedZSet.ofSeq [(1, "a", 3); (2, "b", -2)]
        let negated = IndexedZSet.negate indexed
        
        Assert.That(negated.GetZSet(1).GetWeight("a"), Is.EqualTo -3)
        Assert.That(negated.GetZSet(2).GetWeight("b"), Is.EqualTo 2)

[<TestFixture>]
type IndexedZSetPropertyTests() =

    [<Test>]
    member _.``IndexedZSet addition is commutative``() =
        let property (data1: (int * string * int) list) (data2: (int * string * int) list) =
            let indexed1 = IndexedZSet.ofSeq data1
            let indexed2 = IndexedZSet.ofSeq data2
            let result1 = IndexedZSet.add indexed1 indexed2
            let result2 = IndexedZSet.add indexed2 indexed1
            
            // Get all keys that appear in either result
            let allKeys = 
                [result1; result2]
                |> List.collect (fun idx -> idx.IndexKeys |> Seq.toList)
                |> List.distinct
            
            // Check commutativity for all keys
            allKeys |> List.forall (fun k ->
                let zset1 = result1.GetZSet(k)
                let zset2 = result2.GetZSet(k)
                
                // Check all values in both zsets
                let allValues = 
                    [ZSet.toSeq zset1; ZSet.toSeq zset2]
                    |> Seq.collect id
                    |> Seq.map fst
                    |> Seq.distinct
                    |> Seq.toList
                
                allValues |> List.forall (fun v ->
                    zset1.GetWeight(v) = zset2.GetWeight(v))
            )
        
        Check.QuickThrowOnFailure property
