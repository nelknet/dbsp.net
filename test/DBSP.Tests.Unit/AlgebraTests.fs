/// Unit tests for DBSP.Core.Algebra module
module AlgebraTests

open NUnit.Framework
open FsCheck
open DBSP.Core.Algebra

/// Test SRTP-based algebraic operations with basic numeric types
[<TestFixture>]
type AlgebraOperationTests() =

    [<Test>]
    member _.``add function works with integers``() =
        let result = add 5 3
        Assert.AreEqual(8, result)

    [<Test>]
    member _.``add function works with floating point``() =
        let result = add 5.5 3.3
        Assert.AreEqual(8.8, result, 0.001)

    [<Test>]
    member _.``zero function works with integers``() =
        let result = zero<int>
        Assert.AreEqual(0, result)

    [<Test>]
    member _.``one function works with integers``() =
        let result = one<int>
        Assert.AreEqual(1, result)

    [<Test>]
    member _.``negate function works with integers``() =
        let result = negate 5
        Assert.AreEqual(-5, result)

    [<Test>]
    member _.``multiply function works with integers``() =
        let result = multiply 4 3
        Assert.AreEqual(12, result)

    [<Test>]
    member _.``subtract function works with integers``() =
        let result = subtract 10 3
        Assert.AreEqual(7, result)

    [<Test>]
    member _.``scalarMultiply function works``() =
        let result = scalarMultiply 3 5
        Assert.AreEqual(15, result)

    [<Test>]
    member _.``addOpt handles Some values correctly``() =
        let result = addOpt<int> (Some 5) (Some 3)
        Assert.AreEqual(Some 8, result)

    [<Test>]
    member _.``addOpt handles None values correctly``() =
        let result1 = addOpt<int> (Some 5) None
        let result2 = addOpt<int> None (Some 3)
        let result3 = addOpt<int> None None
        
        Assert.AreEqual(Some 5, result1)
        Assert.AreEqual(Some 3, result2)
        Assert.AreEqual(None, result3)

/// Property-based tests for algebraic laws using FsCheck
[<TestFixture>]
type AlgebraPropertyTests() =

    [<Test>]
    member _.``Addition is commutative for integers``() =
        let property (a: int) (b: int) =
            add a b = add b a
        Check.QuickThrowOnFailure property

    [<Test>]
    member _.``Addition is associative for integers``() =
        let property (a: int) (b: int) (c: int) =
            add (add a b) c = add a (add b c)
        Check.QuickThrowOnFailure property

    [<Test>]
    member _.``Zero is additive identity``() =
        let property (a: int) =
            add a (zero<int>) = a && add (zero<int>) a = a
        Check.QuickThrowOnFailure property

    [<Test>]
    member _.``Negation is additive inverse``() =
        let property (a: int) =
            add a (negate a) = zero<int>
        Check.QuickThrowOnFailure property

    [<Test>]
    member _.``Multiplication distributes over addition``() =
        let property (a: int) (b: int) (c: int) =
            multiply a (add b c) = add (multiply a b) (multiply a c)
        Check.QuickThrowOnFailure property

    [<Test>]
    member _.``One is multiplicative identity``() =
        let property (a: int) =
            multiply a (one<int>) = a && multiply (one<int>) a = a
        Check.QuickThrowOnFailure property