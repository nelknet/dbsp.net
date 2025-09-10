/// Unit tests for DBSP.Core.ZSet module
module ZSetTests

open NUnit.Framework
open FsCheck
open FsCheck.NUnit
open DBSP.Core.ZSet

[<TestFixture>]
type ZSetBasicOperationTests() =

    [<Test>]
    member _.``empty ZSet is indeed empty``() =
        let zset = ZSet.empty<int>
        Assert.That(zset.IsEmpty, Is.True)
        Assert.That(zset.Count, Is.EqualTo 0)

    [<Test>]
    member _.``singleton ZSet contains single element``() =
        let zset = ZSet.singleton 42 5
        Assert.That(zset.IsEmpty, Is.False)
        Assert.That(zset.GetWeight(42), Is.EqualTo 5)
        Assert.That(zset.Count, Is.EqualTo 5)

    [<Test>]
    member _.``ofList creates ZSet correctly``() =
        let pairs = [(1, 3); (2, -2); (3, 1)]
        let zset = ZSet.ofList pairs
        
        Assert.That(zset.GetWeight(1), Is.EqualTo 3)
        Assert.That(zset.GetWeight(2), Is.EqualTo -2)
        Assert.That(zset.GetWeight(3), Is.EqualTo 1)
        Assert.That(zset.GetWeight(4), Is.EqualTo 0) // Non-existent key

    [<Test>]
    member _.``insert adds to existing weight``() =
        let zset = ZSet.singleton 42 5
        let updated = ZSet.insert 42 3 zset
        Assert.That(updated.GetWeight(42), Is.EqualTo 8)

    [<Test>]
    member _.``insert with zero weight removes element``() =
        let zset = ZSet.singleton 42 5
        let updated = ZSet.insert 42 (-5) zset
        Assert.That(updated.IsEmpty, Is.True)
        Assert.That(updated.GetWeight(42), Is.EqualTo 0)

    [<Test>]
    member _.``remove subtracts weight``() =
        let zset = ZSet.singleton 42 5
        let updated = ZSet.remove 42 2 zset
        Assert.That(updated.GetWeight(42), Is.EqualTo 3)

    [<Test>]
    member _.``filter works correctly``() =
        let pairs = [(1, 3); (2, -2); (3, 1); (4, 2)]
        let zset = ZSet.ofList pairs
        let filtered = ZSet.filter (fun k -> k % 2 = 0) zset
        
        Assert.That(filtered.GetWeight(1), Is.EqualTo 0)
        Assert.That(filtered.GetWeight(2), Is.EqualTo -2)
        Assert.That(filtered.GetWeight(3), Is.EqualTo 0)
        Assert.That(filtered.GetWeight(4), Is.EqualTo 2)

    [<Test>]
    member _.``mapKeys transforms keys correctly``() =
        let pairs = [(1, 3); (2, -2); (3, 1)]
        let zset = ZSet.ofList pairs
        let mapped = ZSet.mapKeys (fun k -> k * 10) zset
        
        Assert.That(mapped.GetWeight(10), Is.EqualTo 3)
        Assert.That(mapped.GetWeight(20), Is.EqualTo -2)
        Assert.That(mapped.GetWeight(30), Is.EqualTo 1)

[<TestFixture>]
type ZSetAlgebraicOperationTests() =

    [<Test>]
    member _.``ZSet addition combines weights correctly``() =
        let zset1 = ZSet.ofList [(1, 3); (2, -2)]
        let zset2 = ZSet.ofList [(1, 2); (3, 4)]
        let result = ZSet.add zset1 zset2
        
        Assert.That(result.GetWeight(1), Is.EqualTo 5)  // 3 + 2
        Assert.That(result.GetWeight(2), Is.EqualTo -2) // -2 + 0
        Assert.That(result.GetWeight(3), Is.EqualTo 4)  // 0 + 4

    [<Test>]
    member _.``ZSet negation flips all weights``() =
        let zset = ZSet.ofList [(1, 3); (2, -2); (3, 0)]
        let negated = ZSet.negate zset
        
        Assert.That(negated.GetWeight(1), Is.EqualTo -3)
        Assert.That(negated.GetWeight(2), Is.EqualTo 2)
        Assert.That(negated.GetWeight(3), Is.EqualTo 0)

    [<Test>]
    member _.``ZSet scalar multiplication scales all weights``() =
        let zset = ZSet.ofList [(1, 3); (2, -2)]
        let scaled = ZSet.scalarMultiply 3 zset
        
        Assert.That(scaled.GetWeight(1), Is.EqualTo 9)   // 3 * 3
        Assert.That(scaled.GetWeight(2), Is.EqualTo -6)  // 3 * -2

    [<Test>]
    member _.``ZSet union equals addition``() =
        let zset1 = ZSet.ofList [(1, 3); (2, -2)]
        let zset2 = ZSet.ofList [(1, 2); (3, 4)]
        let unionResult = ZSet.union zset1 zset2
        let addResult = ZSet.add zset1 zset2
        
        Assert.That(addResult.GetWeight(1), Is.EqualTo (unionResult.GetWeight(1)))
        Assert.That(addResult.GetWeight(2), Is.EqualTo (unionResult.GetWeight(2)))
        Assert.That(addResult.GetWeight(3), Is.EqualTo (unionResult.GetWeight(3)))

    [<Test>]
    member _.``ZSet difference using negation``() =
        let zset1 = ZSet.ofList [(1, 5); (2, 3)]
        let zset2 = ZSet.ofList [(1, 2); (2, 1)]
        let diff = ZSet.difference zset1 zset2
        
        Assert.That(diff.GetWeight(1), Is.EqualTo 3)  // 5 - 2
        Assert.That(diff.GetWeight(2), Is.EqualTo 2)  // 3 - 1

/// Property-based tests for ZSet algebraic laws
[<TestFixture>]
type ZSetPropertyTests() =

    [<FsCheck.NUnit.Property>]
    member _.``ZSet addition is commutative``(data1: (int * int) list, data2: (int * int) list) =
        let zset1 = ZSet.ofList data1
        let zset2 = ZSet.ofList data2
        let result1 = ZSet.add zset1 zset2
        let result2 = ZSet.add zset2 zset1
        
        // Check that weights are equal for all possible keys
        let allKeys = 
            (ZSet.toSeq result1 |> Seq.map fst) 
            |> Seq.append (ZSet.toSeq result2 |> Seq.map fst)
            |> Seq.distinct
        
        allKeys |> Seq.forall (fun k -> result1.GetWeight(k) = result2.GetWeight(k))

    [<FsCheck.NUnit.Property>]
    member _.``ZSet addition is associative``(data1: (int * int) list, data2: (int * int) list, data3: (int * int) list) =
        let zset1 = ZSet.ofList data1
        let zset2 = ZSet.ofList data2
        let zset3 = ZSet.ofList data3
        let left = ZSet.add (ZSet.add zset1 zset2) zset3
        let right = ZSet.add zset1 (ZSet.add zset2 zset3)
        // Check associativity for all keys
        let allKeys = 
            [left; right]
            |> List.collect (fun zs -> ZSet.toSeq zs |> Seq.map fst |> Seq.toList)
            |> List.distinct
        allKeys |> List.forall (fun k -> left.GetWeight(k) = right.GetWeight(k))

    [<FsCheck.NUnit.Property>]
    member _.``ZSet has additive identity (zero)``(data: (int * int) list) =
        let zset = ZSet.ofList data
        let zero = ZSet.empty<int>
        let leftResult = ZSet.add zset zero
        let rightResult = ZSet.add zero zset
        let keys = ZSet.toSeq zset |> Seq.map fst |> Seq.toList
        keys |> List.forall (fun k ->
            zset.GetWeight(k) = leftResult.GetWeight(k) &&
            zset.GetWeight(k) = rightResult.GetWeight(k))

    [<FsCheck.NUnit.Property>]
    member _.``ZSet negation is additive inverse``(data: (int * int) list) =
        let zset = ZSet.ofList data
        let negated = ZSet.negate zset
        let sum = ZSet.add zset negated
        
        // Result should be empty (all weights zero)
        ZSet.toSeq sum |> Seq.isEmpty
