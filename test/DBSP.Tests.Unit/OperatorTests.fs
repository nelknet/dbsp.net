/// Unit tests for DBSP operator implementations  
module OperatorTests

open System.Threading.Tasks
open NUnit.Framework
open DBSP.Core.ZSet
open DBSP.Core.IndexedZSet
open DBSP.Operators.Interfaces
open DBSP.Operators.LinearOperators
open DBSP.Operators.JoinOperators
open DBSP.Operators.AggregateOperators

[<TestFixture>]
type BasicOperatorTests() =

    [<Test>]
    member _.``MapOperator interface works`` () =
        let mapOp = MapOperator(fun x -> x * 2) :> IOperator
        Assert.That(mapOp.Name, Is.EqualTo "Map")
        Assert.That(mapOp.IsAsync, Is.False)

    [<Test>]
    member _.``UnionOperator interface works`` () =
        let unionOp = UnionOperator() :> IOperator
        Assert.That(unionOp.Name, Is.EqualTo "Union")
        Assert.That(unionOp.IsAsync, Is.False)

    [<Test>]
    member _.``InnerJoinOperator interface works`` () =
        let joinOp = InnerJoinOperator() :> IOperator
        Assert.That(joinOp.Name, Is.EqualTo "InnerJoin")
        Assert.That(joinOp.IsAsync, Is.False)

    [<Test>]
    member _.``Operators have correct metadata`` () =
        let mapOp = MapOperator(fun x -> x.ToString()) :> IOperator
        let filterOp = ZSetFilterOperator(fun x -> x > 0) :> IOperator
        let groupOp = GroupByOperator(fun x -> x % 2) :> IOperator
        
        Assert.That(mapOp.Name, Is.EqualTo "Map")
        Assert.That(filterOp.Name, Is.EqualTo "ZSetFilter")
        Assert.That(groupOp.Name, Is.EqualTo "GroupBy")

[<TestFixture>]
type OperatorCreationTests() =

    [<Test>]
    member _.``LinearOperators module functions work`` () =
        let mapOp = LinearOperators.map (fun x -> x + 1)
        let filterOp = LinearOperators.filter (fun x -> x > 0)
        let zsetMapOp = LinearOperators.zsetMap (fun x -> x * 2)
        
        Assert.That(mapOp.Name, Is.EqualTo "Map")
        Assert.That(filterOp.Name, Is.EqualTo "Filter")
        Assert.That(zsetMapOp.Name, Is.EqualTo "ZSetMap")

    [<Test>]
    member _.``JoinOperators module functions work`` () =
        let innerJoinOp = JoinOperators.innerJoin
        let semiJoinOp = JoinOperators.semiJoin
        let antiJoinOp = JoinOperators.antiJoin
        
        Assert.That(innerJoinOp.Name, Is.EqualTo "InnerJoin")
        Assert.That(semiJoinOp.Name, Is.EqualTo "SemiJoin")
        Assert.That(antiJoinOp.Name, Is.EqualTo "AntiJoin")

    [<Test>]
    member _.``AggregateOperators module functions work`` () =
        let countOp = AggregateOperators.count
        let intSumOp = AggregateOperators.intSum
        let floatSumOp = AggregateOperators.floatSum
        let avgOp = AggregateOperators.average
        
        Assert.That(countOp.Name, Is.EqualTo "Count")
        Assert.That(intSumOp.Name, Is.EqualTo "IntSum")
        Assert.That(floatSumOp.Name, Is.EqualTo "FloatSum")
        Assert.That(avgOp.Name, Is.EqualTo "Average")

[<TestFixture>]
type SimpleAsyncTests() =

    [<Test>]
    member _.``Simple map operation works asynchronously`` () = task {
        let mapOp = MapOperator(fun x -> x * 2) :> IUnaryOperator<int, int>
        let! result = mapOp.EvalAsync(5)
        Assert.That(result, Is.EqualTo 10)
    }

    [<Test>]
    member _.``Simple ZSet operations work`` () = task {
        let zset = ZSet.ofList [(1, 3); (2, -2); (3, 1)]
        let filterOp = ZSetFilterOperator(fun x -> x > 0) :> IUnaryOperator<ZSet<int>, ZSet<int>>
        let! result = filterOp.EvalAsync(zset)
        
        // Filter keeps keys where the key > 0, preserving original weights
        Assert.That(result.GetWeight(1), Is.EqualTo 3)   // Key 1 > 0, weight 3 preserved
        Assert.That(result.GetWeight(2), Is.EqualTo -2)  // Key 2 > 0, weight -2 preserved  
        Assert.That(result.GetWeight(3), Is.EqualTo 1)   // Key 3 > 0, weight 1 preserved
    }

[<TestFixture>]
type StatefulOperatorTests() =

    [<Test>]
    member _.``Stateful operators maintain state`` () = task {
        let joinOp = InnerJoinOperator()
        let statefulOp = joinOp :> IStatefulOperator<IndexedZSet<int, string> * IndexedZSet<int, int>>
        
        // Initially empty state
        let (leftState, rightState) = statefulOp.GetState()
        Assert.That(leftState.IsEmpty, Is.True)
        Assert.That(rightState.IsEmpty, Is.True)
        
        // Process some data
        let left = ZSet.ofList [((1, "a"), 1)]
        let right = ZSet.ofList [((1, 10), 1)]
        let joinOpInterface = joinOp :> IBinaryOperator<ZSet<int * string>, ZSet<int * int>, IndexedZSet<int, string * int>>
        let! _ = joinOpInterface.EvalAsync left right
        
        // State should now contain the processed data
        let (newLeftState, newRightState) = statefulOp.GetState()
        Assert.That(newLeftState.IsEmpty, Is.False)
        Assert.That(newRightState.IsEmpty, Is.False)
    }
