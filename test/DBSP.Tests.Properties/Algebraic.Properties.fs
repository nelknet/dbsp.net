/// Property-based tests for algebraic law validation
/// Ensures mathematical correctness of DBSP algebraic operations using FsCheck
module DBSP.Tests.Properties.Algebraic

open System
open NUnit.Framework
open FsCheck
open FsCheck.FSharp
open FsCheck.NUnit
open DBSP.Core.ZSet
open DBSP.Core.Algebra
open DBSP.Tests.Properties.Generators
open DBSP.Tests.Properties.TestConfiguration
/// Arbitrary providers for numeric types available to attributes below
type NumericArbs =
    static member FiniteFloat() : Arbitrary<float> =
        let genFinite =
            gen {
                let! i = Gen.choose(-100000, 100000)
                let! frac = Gen.choose(0, 1000000)
                let sign = if i < 0 then -1.0 else 1.0
                return (float i) + sign * (float frac / 1000000.0)
            }
        Arb.fromGen genFinite

type AlgebraArbs =
    static member SmallInt() : Arbitrary<int> =
        Arb.fromGen (Gen.choose(-5,5))

/// Group Laws (Additive Group) - Core algebraic structure for Z-sets
[<TestFixture>]
type GroupLawProperties() =
    
    /// Test associativity: (a + b) + c = a + (b + c)
    [<FsCheck.NUnit.Property(Arbitrary = [| typeof<ZSetGenerators> |])>]
    member _.``ZSet addition is associative``(a: ZSet<int>, b: ZSet<int>, c: ZSet<int>) =
        let left = ZSet.add (ZSet.add a b) c
        let right = ZSet.add a (ZSet.add b c)
        left = right
    
    /// Test commutativity: a + b = b + a  
    [<FsCheck.NUnit.Property(Arbitrary = [| typeof<ZSetGenerators> |])>]
    member _.``ZSet addition is commutative``(a: ZSet<int>, b: ZSet<int>) =
        let left = ZSet.add a b
        let right = ZSet.add b a
        left = right
    
    /// Test identity: a + 0 = a
    [<FsCheck.NUnit.Property(Arbitrary = [| typeof<ZSetGenerators> |])>]
    member _.``ZSet has additive identity``(a: ZSet<int>) =
        let zero = ZSet<int>.Zero
        let result = ZSet.add a zero
        result = a && ZSet.add zero a = a
    
    /// Test inverse: a + (-a) = 0
    [<FsCheck.NUnit.Property(Arbitrary = [| typeof<ZSetGenerators> |])>]
    member _.``ZSet has additive inverse``(a: ZSet<int>) =
        let negA = ZSet.negate a
        let result = ZSet.add a negA
        result = ZSet<int>.Zero
    
    /// Test that negation is involutive: -(-a) = a
    [<FsCheck.NUnit.Property(Arbitrary = [| typeof<ZSetGenerators> |])>]
    member _.``ZSet double negation is identity``(a: ZSet<int>) =
        let negNegA = ZSet.negate (ZSet.negate a)
        negNegA = a
    
    /// Test scalar multiplication distributivity: s * (a + b) = s * a + s * b
    [<FsCheck.NUnit.Property(Arbitrary = [| typeof<ZSetGenerators>; typeof<AlgebraArbs> |])>]
    member _.``Scalar multiplication distributes over addition``(s: int, a: ZSet<int>, b: ZSet<int>) =
        let left = ZSet.scalarMultiply s (ZSet.add a b)
        let right = ZSet.add (ZSet.scalarMultiply s a) (ZSet.scalarMultiply s b)
        // Compare modulo zero-weight entries
        let l = ZSet.toSeq left |> Seq.sort |> Seq.toList
        let r = ZSet.toSeq right |> Seq.sort |> Seq.toList
        let ok = (l = r)
        if not ok then
            NUnit.Framework.TestContext.WriteLine(sprintf "Counterexample: s=%A, left=%A, right=%A, a=%A, b=%A" s l r (ZSet.toSeq a |> Seq.toList) (ZSet.toSeq b |> Seq.toList))
        ok
    
    /// Test scalar multiplication associativity: (s * t) * a = s * (t * a)
    [<FsCheck.NUnit.Property(Arbitrary = [| typeof<ZSetGenerators> |])>]
    member _.``Scalar multiplication is associative``(s: int, t: int, a: ZSet<int>) =
        let left = ZSet.scalarMultiply (s * t) a
        let right = ZSet.scalarMultiply s (ZSet.scalarMultiply t a)
        left = right
    
    /// Test scalar identity: 1 * a = a
    [<FsCheck.NUnit.Property(Arbitrary = [| typeof<ZSetGenerators> |])>]
    member _.``Scalar multiplication has identity``(a: ZSet<int>) =
        ZSet.scalarMultiply 1 a = a
    
    /// Test scalar zero: 0 * a = 0
    [<FsCheck.NUnit.Property(Arbitrary = [| typeof<ZSetGenerators> |])>]
    member _.``Scalar multiplication by zero gives zero``(a: ZSet<int>) =
        // Zero scalar leaves only zero-weights; treat as empty under Z-set semantics
        (ZSet.scalarMultiply 0 a).IsEmpty

/// Ring-like Properties - Tests for additional algebraic structure
[<TestFixture>]  
type RingLikeProperties() =
    
    /// Test that union is the same as addition for Z-sets
    [<FsCheck.NUnit.Property(Arbitrary = [| typeof<ZSetGenerators> |])>]
    member _.``Union equals addition``(a: ZSet<int>, b: ZSet<int>) =
        ZSet.union a b = ZSet.add a b
    
    /// Test that difference uses negation correctly: a - b = a + (-b)
    [<FsCheck.NUnit.Property(Arbitrary = [| typeof<ZSetGenerators> |])>]
    member _.``Difference uses negation``(a: ZSet<int>, b: ZSet<int>) =
        ZSet.difference a b = ZSet.add a (ZSet.negate b)

/// SRTP Algebraic Function Properties - Tests for zero-cost abstractions
[<TestFixture>]
type SRTPAlgebraProperties() =
    
    /// Test that SRTP add function works correctly with ZSet
    [<FsCheck.NUnit.Property(Arbitrary = [| typeof<ZSetGenerators> |])>]
    member _.``SRTP add function works with ZSet``(a: ZSet<int>, b: ZSet<int>) =
        let srtpResult = add a b
        let directResult = a + b
        srtpResult = directResult
    
    /// Test that SRTP negate function works correctly with ZSet
    [<FsCheck.NUnit.Property(Arbitrary = [| typeof<ZSetGenerators> |])>]
    member _.``SRTP negate function works with ZSet``(a: ZSet<int>) =
        let srtpResult = negate a
        let directResult = -a
        srtpResult = directResult
    
    /// Test that SRTP zero function works correctly with ZSet
    [<Test>]
    member _.``SRTP zero function works with ZSet``() =
        let srtpResult = zero<ZSet<int>>
        let directResult = ZSet<int>.Zero
        Assert.That(srtpResult, Is.EqualTo directResult)

/// ZSet Structural Properties - Tests for data structure invariants
[<TestFixture>]
type ZSetStructuralProperties() =
    
    /// Test that weight retrieval is consistent
    [<Property(Arbitrary = [| typeof<ZSetGenerators> |])>]
    member _.``Weight retrieval is consistent``(zset: ZSet<int>, key: int) =
        let weight = zset.GetWeight(key)
        let reconstructed = ZSet.singleton key weight
        let afterSubtraction = ZSet.difference zset reconstructed
        afterSubtraction.GetWeight(key) = 0
    
    /// Test that empty ZSet behaves correctly
    [<Test>]
    member _.``Empty ZSet properties``() =
        let empty = ZSet.empty<int>
        Assert.That(empty.IsEmpty, Is.True)
        Assert.That(empty.Count, Is.EqualTo 0)
        Assert.That(empty.GetWeight(42), Is.EqualTo 0)
        Assert.That(Seq.isEmpty empty.Keys, Is.True)
    
    /// Test that singleton ZSet behaves correctly
    [<FsCheck.NUnit.Property(Arbitrary = [| typeof<ZSetGenerators> |])>]
    member _.``Singleton ZSet properties``(key: int, weight: int) =
        let singleton = ZSet.singleton key weight
        if weight = 0 then
            singleton.IsEmpty
        else
            singleton.GetWeight(key) = weight &&
            singleton.Count = abs weight &&
            (Seq.length singleton.Keys = 1)
    
    /// Test that filtering preserves algebraic structure
    [<FsCheck.NUnit.Property(Arbitrary = [| typeof<ZSetGenerators> |])>]
    member _.``Filter preserves addition when predicates align``(zset1: ZSet<int>, zset2: ZSet<int>) =
        let predicate x = x % 2 = 0 // Even numbers only
        let filteredSum = ZSet.filter predicate (ZSet.add zset1 zset2)
        let sumFiltered = ZSet.add (ZSet.filter predicate zset1) (ZSet.filter predicate zset2)
        filteredSum = sumFiltered
    
    /// Test that mapping keys preserves weights
    [<FsCheck.NUnit.Property(Arbitrary = [| typeof<ZSetGenerators> |])>]
    member _.``Map keys preserves total weight magnitude``(zset: ZSet<int>) =
        let mapped = ZSet.mapKeys (fun x -> x + 1000) zset // Shift keys to avoid collisions
        let originalSum = ZSet.fold (fun acc _ weight -> acc + abs weight) 0 zset
        let mappedSum = ZSet.fold (fun acc _ weight -> acc + abs weight) 0 mapped
        originalSum = mappedSum

/// Edge Case Properties
[<TestFixture>]
type EdgeCaseProperties() =
    
    /// Test that zero weights are handled correctly
    [<FsCheck.NUnit.Property(Arbitrary = [| typeof<ZSetGenerators> |])>]
    member _.``Zero weights are filtered out``(key: int) =
        let zset = ZSet.singleton key 0
        zset.IsEmpty && not (Seq.contains key zset.Keys)
    
    /// Test string keys work with all operations
    [<FsCheck.NUnit.Property(Arbitrary = [| typeof<ZSetGenerators> |])>]
    member _.``String key ZSets follow algebraic laws``(a: ZSet<string>, b: ZSet<string>) =
        // Test basic associativity with string keys
        let c = ZSet.singleton "test" 1
        let left = ZSet.add (ZSet.add a b) c
        let right = ZSet.add a (ZSet.add b c)
        left = right

/// Integration with existing numeric types
[<TestFixture>]
type NumericIntegrationProperties() =
    
    /// Test that regular integers follow algebraic laws with SRTP functions
    [<FsCheck.NUnit.Property(Arbitrary = [| typeof<ZSetGenerators> |])>]
    member _.``SRTP functions work with integers``(a: int, b: int) =
        let srtpSum = add a b
        let directSum = a + b
        srtpSum = directSum
    
    /// Test that floating point numbers work with SRTP
    [<FsCheck.NUnit.Property(Arbitrary = [| typeof<ZSetGenerators>; typeof<NumericArbs> |])>]
    member _.``SRTP functions work with floats``(a: float, b: float) =
        let srtpSum = add a b
        let directSum = a + b
        abs (srtpSum - directSum) < 1e-10 // Handle floating point precision
