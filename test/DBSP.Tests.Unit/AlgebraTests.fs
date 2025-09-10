/// Unit tests for DBSP.Core.Algebra module
module AlgebraTests

open NUnit.Framework
open FsCheck
open FsCheck.NUnit
open DBSP.Core.Algebra

/// Test SRTP-based algebraic operations with basic numeric types
[<TestFixture>]
type AlgebraOperationTests() =

    [<Test>]
    member _.``add function works with integers``() =
        let result = add 5 3
        Assert.That(result, Is.EqualTo 8)

    [<Test>]
    member _.``add function works with floating point``() =
        let result = add 5.5 3.3
        Assert.That(result, Is.EqualTo(8.8).Within(0.001))

    [<Test>]
    member _.``zero function works with integers``() =
        let result = zero<int>
        Assert.That(result, Is.EqualTo 0)

    [<Test>]
    member _.``one function works with integers``() =
        let result = one<int>
        Assert.That(result, Is.EqualTo 1)

    [<Test>]
    member _.``negate function works with integers``() =
        let result = negate 5
        Assert.That(result, Is.EqualTo -5)

    [<Test>]
    member _.``multiply function works with integers``() =
        let result = multiply 4 3
        Assert.That(result, Is.EqualTo 12)

    [<Test>]
    member _.``subtract function works with integers``() =
        let result = subtract 10 3
        Assert.That(result, Is.EqualTo 7)

    [<Test>]
    member _.``scalarMultiply function works``() =
        let result = scalarMultiply 3 5
        Assert.That(result, Is.EqualTo 15)

    [<Test>]
    member _.``addOpt handles Some values correctly``() =
        let result = addOpt<int> (Some 5) (Some 3)
        Assert.That(result, Is.EqualTo (Some 8))

    [<Test>]
    member _.``addOpt handles None values correctly``() =
        let result1 = addOpt<int> (Some 5) None
        let result2 = addOpt<int> None (Some 3)
        let result3 = addOpt<int> None None
        
        Assert.That(result1, Is.EqualTo (Some 5))
        Assert.That(result2, Is.EqualTo (Some 3))
        Assert.That(result3, Is.EqualTo None)

/// Property-based tests for algebraic laws using FsCheck
[<TestFixture>]
type AlgebraPropertyTests() =

    [<FsCheck.NUnit.Property>]
    member _.``Addition is commutative for integers``(a: int, b: int) =
        add a b = add b a

    [<FsCheck.NUnit.Property>]
    member _.``Addition is associative for integers``(a: int, b: int, c: int) =
        add (add a b) c = add a (add b c)

    [<FsCheck.NUnit.Property>]
    member _.``Zero is additive identity``(a: int) =
        add a (zero<int>) = a && add (zero<int>) a = a

    [<FsCheck.NUnit.Property>]
    member _.``Negation is additive inverse``(a: int) =
        add a (negate a) = zero<int>

    [<FsCheck.NUnit.Property>]
    member _.``Multiplication distributes over addition``(a: int, b: int, c: int) =
        multiply a (add b c) = add (multiply a b) (multiply a c)

    [<FsCheck.NUnit.Property>]
    member _.``One is multiplicative identity``(a: int) =
        multiply a (one<int>) = a && multiply (one<int>) a = a
