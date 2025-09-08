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
        Assert.AreEqual("Map", mapOp.Name)
        Assert.IsFalse(mapOp.IsAsync)

    [<Test>]
    member _.``UnionOperator interface works`` () =
        let unionOp = UnionOperator() :> IOperator
        Assert.AreEqual("Union", unionOp.Name)
        Assert.IsFalse(unionOp.IsAsync)

    [<Test>]
    member _.``InnerJoinOperator interface works`` () =
        let joinOp = InnerJoinOperator() :> IOperator
        Assert.AreEqual("InnerJoin", joinOp.Name)
        Assert.IsFalse(joinOp.IsAsync)

    [<Test>]
    member _.``Operators have correct metadata`` () =
        let mapOp = MapOperator(fun x -> x.ToString()) :> IOperator
        let filterOp = ZSetFilterOperator(fun x -> x > 0) :> IOperator
        let groupOp = GroupByOperator(fun x -> x % 2) :> IOperator
        
        Assert.AreEqual("Map", mapOp.Name)
        Assert.AreEqual("ZSetFilter", filterOp.Name)
        Assert.AreEqual("GroupBy", groupOp.Name)

[<TestFixture>]
type OperatorCreationTests() =

    [<Test>]
    member _.``LinearOperators module functions work`` () =
        let mapOp = LinearOperators.map (fun x -> x + 1)
        let filterOp = LinearOperators.filter (fun x -> x > 0)
        let zsetMapOp = LinearOperators.zsetMap (fun x -> x * 2)
        
        Assert.AreEqual("Map", mapOp.Name)
        Assert.AreEqual("Filter", filterOp.Name)
        Assert.AreEqual("ZSetMap", zsetMapOp.Name)

    [<Test>]
    member _.``JoinOperators module functions work`` () =
        let innerJoinOp = JoinOperators.innerJoin
        let semiJoinOp = JoinOperators.semiJoin
        let antiJoinOp = JoinOperators.antiJoin
        
        Assert.AreEqual("InnerJoin", innerJoinOp.Name)
        Assert.AreEqual("SemiJoin", semiJoinOp.Name)
        Assert.AreEqual("AntiJoin", antiJoinOp.Name)

    [<Test>]
    member _.``AggregateOperators module functions work`` () =
        let countOp = AggregateOperators.count
        let intSumOp = AggregateOperators.intSum
        let floatSumOp = AggregateOperators.floatSum
        let avgOp = AggregateOperators.average
        
        Assert.AreEqual("Count", countOp.Name)
        Assert.AreEqual("IntSum", intSumOp.Name)
        Assert.AreEqual("FloatSum", floatSumOp.Name)
        Assert.AreEqual("Average", avgOp.Name)

[<TestFixture>]
type SimpleAsyncTests() =

    [<Test>]
    member _.``Simple map operation works asynchronously`` () = task {
        let mapOp = MapOperator(fun x -> x * 2) :> IUnaryOperator<int, int>
        let! result = mapOp.EvalAsync(5)
        Assert.AreEqual(10, result)
    }

    [<Test>]
    member _.``Simple ZSet operations work`` () = task {
        let zset = ZSet.ofList [(1, 3); (2, -2); (3, 1)]
        let filterOp = ZSetFilterOperator(fun x -> x > 0) :> IUnaryOperator<ZSet<int>, ZSet<int>>
        let! result = filterOp.EvalAsync(zset)
        
        // Filter keeps keys where the key > 0, preserving original weights
        Assert.AreEqual(3, result.GetWeight(1))   // Key 1 > 0, weight 3 preserved
        Assert.AreEqual(-2, result.GetWeight(2))  // Key 2 > 0, weight -2 preserved  
        Assert.AreEqual(1, result.GetWeight(3))   // Key 3 > 0, weight 1 preserved
    }

[<TestFixture>]
type StatefulOperatorTests() =

    [<Test>]
    member _.``Stateful operators maintain state`` () = task {
        let joinOp = InnerJoinOperator()
        let statefulOp = joinOp :> IStatefulOperator<IndexedZSet<int, string> * IndexedZSet<int, int>>
        
        // Initially empty state
        let (leftState, rightState) = statefulOp.GetState()
        Assert.IsTrue(leftState.IsEmpty)
        Assert.IsTrue(rightState.IsEmpty)
        
        // Process some data
        let left = ZSet.ofList [((1, "a"), 1)]
        let right = ZSet.ofList [((1, 10), 1)]
        let joinOpInterface = joinOp :> IBinaryOperator<ZSet<int * string>, ZSet<int * int>, IndexedZSet<int, string * int>>
        let! _ = joinOpInterface.EvalAsync left right
        
        // State should now contain the processed data
        let (newLeftState, newRightState) = statefulOp.GetState()
        Assert.IsFalse(newLeftState.IsEmpty)
        Assert.IsFalse(newRightState.IsEmpty)
    }