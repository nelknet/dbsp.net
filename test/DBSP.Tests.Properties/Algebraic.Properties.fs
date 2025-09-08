/// Property-based tests for algebraic law validation
/// Ensures mathematical correctness of DBSP algebraic operations using FsCheck
module DBSP.Tests.Properties.Algebraic

open System
open NUnit.Framework
open FsCheck
open DBSP.Core.ZSet
open DBSP.Core.Algebra
open DBSP.Tests.Properties.Generators

/// Group Laws (Additive Group) - Core algebraic structure for Z-sets
[<TestFixture>]
type GroupLawProperties() =
    
    let config = Config.Quick.WithArbitrary([typeof<ZSetGenerators>])
    
    /// Test associativity: (a + b) + c = a + (b + c)
    [<Test>]
    member _.``ZSet addition is associative``() =
        let property (a: ZSet<int>) (b: ZSet<int>) (c: ZSet<int>) =
            let left = ZSet.add (ZSet.add a b) c
            let right = ZSet.add a (ZSet.add b c)
            left = right
        Check.One(config, property)
    
    /// Test commutativity: a + b = b + a  
    [<Test>]
    member _.``ZSet addition is commutative``() =
        let property (a: ZSet<int>) (b: ZSet<int>) =
            let left = ZSet.add a b
            let right = ZSet.add b a
            left = right
        Check.One(config, property)
    
    /// Test identity: a + 0 = a
    [<Test>]
    member _.``ZSet has additive identity``() =
        let property (a: ZSet<int>) =
            let zero = ZSet<int>.Zero
            let result = ZSet.add a zero
            result = a && ZSet.add zero a = a
        Check.One(config, property)
    
    /// Test inverse: a + (-a) = 0
    [<Test>]
    member _.``ZSet has additive inverse``() =
        let property (a: ZSet<int>) =
            let negA = ZSet.negate a
            let result = ZSet.add a negA
            result = ZSet<int>.Zero
        Check.One(config, property)
    
    /// Test that negation is involutive: -(-a) = a
    [<Test>]
    member _.``ZSet double negation is identity``() =
        let property (a: ZSet<int>) =
            let negNegA = ZSet.negate (ZSet.negate a)
            negNegA = a
        Check.One(config, property)
    
    /// Test scalar multiplication distributivity: s * (a + b) = s * a + s * b
    [<Test>]
    member _.``Scalar multiplication distributes over addition``() =
        let property (s: int) (a: ZSet<int>) (b: ZSet<int>) =
            let left = ZSet.scalarMultiply s (ZSet.add a b)
            let right = ZSet.add (ZSet.scalarMultiply s a) (ZSet.scalarMultiply s b)
            left = right
        Check.One(config, property)
    
    /// Test scalar multiplication associativity: (s * t) * a = s * (t * a)
    [<Test>]
    member _.``Scalar multiplication is associative``() =
        let property (s: int) (t: int) (a: ZSet<int>) =
            let left = ZSet.scalarMultiply (s * t) a
            let right = ZSet.scalarMultiply s (ZSet.scalarMultiply t a)
            left = right
        Check.One(config, property)
    
    /// Test scalar identity: 1 * a = a
    [<Test>]
    member _.``Scalar multiplication has identity``() =
        let property (a: ZSet<int>) =
            ZSet.scalarMultiply 1 a = a
        Check.One(config, property)
    
    /// Test scalar zero: 0 * a = 0
    [<Test>]
    member _.``Scalar multiplication by zero gives zero``() =
        let property (a: ZSet<int>) =
            ZSet.scalarMultiply 0 a = ZSet<int>.Zero
        Check.One(config, property)

/// Ring-like Properties - Tests for additional algebraic structure
[<TestFixture>]  
type RingLikeProperties() =
    
    /// Test that union is the same as addition for Z-sets
    [<Test>]
    member _.``Union equals addition``() =
        let property (a: ZSet<int>) (b: ZSet<int>) =
            ZSet.union a b = ZSet.add a b
        Check.One(config, property)
    
    /// Test that difference uses negation correctly: a - b = a + (-b)
    [<Test>]
    member _.``Difference uses negation``() =
        let property (a: ZSet<int>) (b: ZSet<int>) =
            ZSet.difference a b = ZSet.add a (ZSet.negate b)
        Check.One(config, property)

/// SRTP Algebraic Function Properties - Tests for zero-cost abstractions
[<TestFixture>]
type SRTPAlgebraProperties() =
    
    let config = Config.Quick.WithArbitrary([typeof<ZSetGenerators>])
    
    /// Test that SRTP add function works correctly with ZSet
    [<Test>]
    member _.``SRTP add function works with ZSet``() =
        let property (a: ZSet<int>) (b: ZSet<int>) =
            let srtpResult = add a b
            let directResult = a + b
            srtpResult = directResult
        Check.One(config, property)
    
    /// Test that SRTP negate function works correctly with ZSet
    [<Test>]
    member _.``SRTP negate function works with ZSet``() =
        let property (a: ZSet<int>) =
            let srtpResult = negate a
            let directResult = -a
            srtpResult = directResult
        Check.One(config, property)
    
    /// Test that SRTP zero function works correctly with ZSet
    [<Test>]
    member _.``SRTP zero function works with ZSet``() =
        let srtpResult = zero<ZSet<int>>
        let directResult = ZSet<int>.Zero
        Assert.AreEqual(directResult, srtpResult)

/// ZSet Structural Properties - Tests for data structure invariants
[<TestFixture>]
type ZSetStructuralProperties() =
    
    let config = Config.Quick.WithArbitrary([typeof<ZSetGenerators>])
    
    /// Test that weight retrieval is consistent
    [<Test>]
    member _.``Weight retrieval is consistent``() =
        let property (zset: ZSet<int>) (key: int) =
            let weight = zset.GetWeight(key)
            let reconstructed = ZSet.singleton key weight
            let afterSubtraction = ZSet.difference zset reconstructed
            afterSubtraction.GetWeight(key) = 0
        Check.One(config, property)
    
    /// Test that empty ZSet behaves correctly
    [<Test>]
    member _.``Empty ZSet properties``() =
        let empty = ZSet.empty<int>
        Assert.IsTrue(empty.IsEmpty)
        Assert.AreEqual(0, empty.Count)
        Assert.AreEqual(0, empty.GetWeight(42))
        Assert.IsTrue(Seq.isEmpty empty.Keys)
    
    /// Test that singleton ZSet behaves correctly
    [<Test>]
    member _.``Singleton ZSet properties``() =
        let property (key: int) (weight: int) =
            let singleton = ZSet.singleton key weight
            if weight = 0 then
                singleton.IsEmpty
            else
                singleton.GetWeight(key) = weight &&
                singleton.Count = abs weight &&
                (Seq.length singleton.Keys = 1)
        Check.One(config, property)
    
    /// Test that filtering preserves algebraic structure
    [<Test>]
    member _.``Filter preserves addition when predicates align``() =
        let property (zset1: ZSet<int>) (zset2: ZSet<int>) =
            let predicate x = x % 2 = 0 // Even numbers only
            let filteredSum = ZSet.filter predicate (ZSet.add zset1 zset2)
            let sumFiltered = ZSet.add (ZSet.filter predicate zset1) (ZSet.filter predicate zset2)
            filteredSum = sumFiltered
        Check.One(config, property)
    
    /// Test that mapping keys preserves weights
    [<Test>]
    member _.``Map keys preserves total weight magnitude``() =
        let property (zset: ZSet<int>) =
            let mapped = ZSet.mapKeys (fun x -> x + 1000) zset // Shift keys to avoid collisions
            let originalSum = ZSet.fold (fun acc _ weight -> acc + abs weight) 0 zset
            let mappedSum = ZSet.fold (fun acc _ weight -> acc + abs weight) 0 mapped
            originalSum = mappedSum
        Check.One(config, property)

/// Edge Case Properties
[<TestFixture>]
type EdgeCaseProperties() =
    
    let config = Config.Quick.WithArbitrary([typeof<ZSetGenerators>])
    
    /// Test that zero weights are handled correctly
    [<Test>]
    member _.``Zero weights are filtered out``() =
        let property (key: int) =
            let zset = ZSet.singleton key 0
            zset.IsEmpty && not (Seq.contains key zset.Keys)
        Check.One(config, property)
    
    /// Test string keys work with all operations
    [<Test>]
    member _.``String key ZSets follow algebraic laws``() =
        let property (a: ZSet<string>) (b: ZSet<string>) =
            // Test basic associativity with string keys
            let c = ZSet.singleton "test" 1
            let left = ZSet.add (ZSet.add a b) c
            let right = ZSet.add a (ZSet.add b c)
            left = right
        Check.One(config, property)

/// Integration with existing numeric types
[<TestFixture>]
type NumericIntegrationProperties() =
    
    let config = Config.Quick.WithArbitrary([typeof<ZSetGenerators>])
    
    /// Test that regular integers follow algebraic laws with SRTP functions
    [<Test>]
    member _.``SRTP functions work with integers``() =
        let property (a: int) (b: int) =
            let srtpSum = add a b
            let directSum = a + b
            srtpSum = directSum
        Check.One(config, property)
    
    /// Test that floating point numbers work with SRTP
    [<Test>]
    member _.``SRTP functions work with floats``() =
        let property (a: float) (b: float) =
            let srtpSum = add a b
            let directSum = a + b
            abs (srtpSum - directSum) < 1e-10 // Handle floating point precision
        Check.One(config, property)