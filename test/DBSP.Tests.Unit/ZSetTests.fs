/// Unit tests for DBSP.Core.ZSet module
module ZSetTests

open NUnit.Framework
open FsCheck
open DBSP.Core.ZSet

[<TestFixture>]
type ZSetBasicOperationTests() =

    [<Test>]
    member _.``empty ZSet is indeed empty``() =
        let zset = ZSet.empty<int>
        Assert.IsTrue(zset.IsEmpty)
        Assert.AreEqual(0, zset.Count)

    [<Test>]
    member _.``singleton ZSet contains single element``() =
        let zset = ZSet.singleton 42 5
        Assert.IsFalse(zset.IsEmpty)
        Assert.AreEqual(5, zset.GetWeight(42))
        Assert.AreEqual(5, zset.Count)

    [<Test>]
    member _.``ofList creates ZSet correctly``() =
        let pairs = [(1, 3); (2, -2); (3, 1)]
        let zset = ZSet.ofList pairs
        
        Assert.AreEqual(3, zset.GetWeight(1))
        Assert.AreEqual(-2, zset.GetWeight(2))
        Assert.AreEqual(1, zset.GetWeight(3))
        Assert.AreEqual(0, zset.GetWeight(4)) // Non-existent key

    [<Test>]
    member _.``insert adds to existing weight``() =
        let zset = ZSet.singleton 42 5
        let updated = ZSet.insert 42 3 zset
        Assert.AreEqual(8, updated.GetWeight(42))

    [<Test>]
    member _.``insert with zero weight removes element``() =
        let zset = ZSet.singleton 42 5
        let updated = ZSet.insert 42 (-5) zset
        Assert.IsTrue(updated.IsEmpty)
        Assert.AreEqual(0, updated.GetWeight(42))

    [<Test>]
    member _.``remove subtracts weight``() =
        let zset = ZSet.singleton 42 5
        let updated = ZSet.remove 42 2 zset
        Assert.AreEqual(3, updated.GetWeight(42))

    [<Test>]
    member _.``filter works correctly``() =
        let pairs = [(1, 3); (2, -2); (3, 1); (4, 2)]
        let zset = ZSet.ofList pairs
        let filtered = ZSet.filter (fun k -> k % 2 = 0) zset
        
        Assert.AreEqual(0, filtered.GetWeight(1))
        Assert.AreEqual(-2, filtered.GetWeight(2))
        Assert.AreEqual(0, filtered.GetWeight(3))
        Assert.AreEqual(2, filtered.GetWeight(4))

    [<Test>]
    member _.``mapKeys transforms keys correctly``() =
        let pairs = [(1, 3); (2, -2); (3, 1)]
        let zset = ZSet.ofList pairs
        let mapped = ZSet.mapKeys (fun k -> k * 10) zset
        
        Assert.AreEqual(3, mapped.GetWeight(10))
        Assert.AreEqual(-2, mapped.GetWeight(20))
        Assert.AreEqual(1, mapped.GetWeight(30))

[<TestFixture>]
type ZSetAlgebraicOperationTests() =

    [<Test>]
    member _.``ZSet addition combines weights correctly``() =
        let zset1 = ZSet.ofList [(1, 3); (2, -2)]
        let zset2 = ZSet.ofList [(1, 2); (3, 4)]
        let result = ZSet.add zset1 zset2
        
        Assert.AreEqual(5, result.GetWeight(1))  // 3 + 2
        Assert.AreEqual(-2, result.GetWeight(2)) // -2 + 0
        Assert.AreEqual(4, result.GetWeight(3))  // 0 + 4

    [<Test>]
    member _.``ZSet negation flips all weights``() =
        let zset = ZSet.ofList [(1, 3); (2, -2); (3, 0)]
        let negated = ZSet.negate zset
        
        Assert.AreEqual(-3, negated.GetWeight(1))
        Assert.AreEqual(2, negated.GetWeight(2))
        Assert.AreEqual(0, negated.GetWeight(3))

    [<Test>]
    member _.``ZSet scalar multiplication scales all weights``() =
        let zset = ZSet.ofList [(1, 3); (2, -2)]
        let scaled = ZSet.scalarMultiply 3 zset
        
        Assert.AreEqual(9, scaled.GetWeight(1))   // 3 * 3
        Assert.AreEqual(-6, scaled.GetWeight(2))  // 3 * -2

    [<Test>]
    member _.``ZSet union equals addition``() =
        let zset1 = ZSet.ofList [(1, 3); (2, -2)]
        let zset2 = ZSet.ofList [(1, 2); (3, 4)]
        let unionResult = ZSet.union zset1 zset2
        let addResult = ZSet.add zset1 zset2
        
        Assert.AreEqual(unionResult.GetWeight(1), addResult.GetWeight(1))
        Assert.AreEqual(unionResult.GetWeight(2), addResult.GetWeight(2))
        Assert.AreEqual(unionResult.GetWeight(3), addResult.GetWeight(3))

    [<Test>]
    member _.``ZSet difference using negation``() =
        let zset1 = ZSet.ofList [(1, 5); (2, 3)]
        let zset2 = ZSet.ofList [(1, 2); (2, 1)]
        let diff = ZSet.difference zset1 zset2
        
        Assert.AreEqual(3, diff.GetWeight(1))  // 5 - 2
        Assert.AreEqual(2, diff.GetWeight(2))  // 3 - 1

/// Property-based tests for ZSet algebraic laws
[<TestFixture>]
type ZSetPropertyTests() =

    [<Test>]
    member _.``ZSet addition is commutative``() =
        let property (data1: (int * int) list) (data2: (int * int) list) =
            let zset1 = ZSet.ofList data1
            let zset2 = ZSet.ofList data2
            let result1 = ZSet.add zset1 zset2
            let result2 = ZSet.add zset2 zset1
            
            // Check that weights are equal for all possible keys
            let allKeys = 
                (ZSet.toSeq result1 |> Seq.map fst) 
                |> Seq.append (ZSet.toSeq result2 |> Seq.map fst)
                |> Seq.distinct
            
            allKeys |> Seq.forall (fun k -> 
                result1.GetWeight(k) = result2.GetWeight(k))
        
        Check.QuickThrowOnFailure property

    [<Test>]
    member _.``ZSet addition is associative``() =
        let property (data1: (int * int) list) (data2: (int * int) list) (data3: (int * int) list) =
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
            
            allKeys |> List.forall (fun k -> 
                left.GetWeight(k) = right.GetWeight(k))
        
        Check.QuickThrowOnFailure property

    [<Test>]
    member _.``ZSet has additive identity (zero)``() =
        let property (data: (int * int) list) =
            let zset = ZSet.ofList data
            let zero = ZSet.empty<int>
            let leftResult = ZSet.add zset zero
            let rightResult = ZSet.add zero zset
            
            let keys = ZSet.toSeq zset |> Seq.map fst |> Seq.toList
            keys |> List.forall (fun k ->
                zset.GetWeight(k) = leftResult.GetWeight(k) &&
                zset.GetWeight(k) = rightResult.GetWeight(k))
        
        Check.QuickThrowOnFailure property

    [<Test>]
    member _.``ZSet negation is additive inverse``() =
        let property (data: (int * int) list) =
            let zset = ZSet.ofList data
            let negated = ZSet.negate zset
            let sum = ZSet.add zset negated
            
            // Result should be empty (all weights zero)
            ZSet.toSeq sum |> Seq.isEmpty
        
        Check.QuickThrowOnFailure property