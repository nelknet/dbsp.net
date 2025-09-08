module DBSP.Tests.Unit.ComplexJoinOperatorTests

open System
open NUnit.Framework
open DBSP.Core.ZSet
open DBSP.Operators.Interfaces
open DBSP.Operators.ComplexJoinOperators
open DBSP.Operators.ComplexJoinOperators.ComplexJoins
open System.Threading.Tasks

[<TestFixture>]
type ComplexJoinOperatorTests() =
    
    /// Helper to create test data
    let createTestData keys values =
        [for k in keys do
         for v in values do
            yield ((k, v), 1)]
        |> ZSet.ofSeq
    
    [<Test>]
    member _.``LeftOuterJoin includes all left records with nulls for unmatched right``() = task {
        let leftOp = leftOuterJoin<int, string, float>()
        
        // Create test data
        let leftData = ZSet.ofSeq [((1, "a"), 1); ((2, "b"), 1); ((3, "c"), 1)]
        let rightData = ZSet.ofSeq [((1, 1.0), 1); ((2, 2.0), 1)]
        
        let! result = (leftOp :> IBinaryOperator<_,_,_>).EvalAsync leftData rightData
        
        // Should have all 3 left records
        let resultList = ZSet.toSeq result |> Seq.sortBy fst |> List.ofSeq
        
        // Debug output
        printfn "Left outer join result: %A" resultList
        
        Assert.AreEqual(3, resultList.Length, sprintf "Expected 3 results but got %d: %A" resultList.Length resultList)
        Assert.IsTrue(resultList |> List.exists (fun ((k, v1, v2), _) -> k = 1 && v1 = "a" && v2 = Some 1.0))
        Assert.IsTrue(resultList |> List.exists (fun ((k, v1, v2), _) -> k = 2 && v1 = "b" && v2 = Some 2.0))
        Assert.IsTrue(resultList |> List.exists (fun ((k, v1, v2), _) -> k = 3 && v1 = "c" && v2 = None))
    }
    
    [<Test>]
    member _.``RightOuterJoin includes all right records with nulls for unmatched left``() = task {
        let rightOp = rightOuterJoin<int, string, float>()
        
        let leftData = ZSet.ofSeq [((1, "a"), 1); ((2, "b"), 1)]
        let rightData = ZSet.ofSeq [((1, 1.0), 1); ((2, 2.0), 1); ((3, 3.0), 1)]
        
        let! result = (rightOp :> IBinaryOperator<_,_,_>).EvalAsync leftData rightData
        
        let resultList = ZSet.toSeq result |> Seq.sortBy fst |> List.ofSeq
        
        Assert.AreEqual(3, resultList.Length)
        Assert.IsTrue(resultList |> List.exists (fun ((k, v1, v2), _) -> k = 1 && v1 = Some "a" && v2 = 1.0))
        Assert.IsTrue(resultList |> List.exists (fun ((k, v1, v2), _) -> k = 2 && v1 = Some "b" && v2 = 2.0))
        Assert.IsTrue(resultList |> List.exists (fun ((k, v1, v2), _) -> k = 3 && v1 = None && v2 = 3.0))
    }
    
    [<Test>]
    member _.``FullOuterJoin includes all records from both sides``() = task {
        let fullOp = fullOuterJoin<int, string, float>()
        
        let leftData = ZSet.ofSeq [((1, "a"), 1); ((2, "b"), 1); ((4, "d"), 1)]
        let rightData = ZSet.ofSeq [((1, 1.0), 1); ((3, 3.0), 1); ((4, 4.0), 1)]
        
        let! result = (fullOp :> IBinaryOperator<_,_,_>).EvalAsync leftData rightData
        
        let resultList = ZSet.toSeq result |> Seq.sortBy fst |> List.ofSeq
        
        Assert.AreEqual(4, resultList.Length)
        Assert.IsTrue(resultList |> List.exists (fun ((k, v1, v2), _) -> k = 1 && v1 = Some "a" && v2 = Some 1.0))
        Assert.IsTrue(resultList |> List.exists (fun ((k, v1, v2), _) -> k = 2 && v1 = Some "b" && v2 = None))
        Assert.IsTrue(resultList |> List.exists (fun ((k, v1, v2), _) -> k = 3 && v1 = None && v2 = Some 3.0))
        Assert.IsTrue(resultList |> List.exists (fun ((k, v1, v2), _) -> k = 4 && v1 = Some "d" && v2 = Some 4.0))
    }
    
    [<Test>]
    member _.``SemiJoin returns only left records with matching keys in right``() = task {
        let semiOp = semiJoin<int, string, float>()
        
        let leftData = ZSet.ofSeq [((1, "a"), 1); ((2, "b"), 1); ((3, "c"), 1)]
        let rightData = ZSet.ofSeq [((1, 1.0), 1); ((3, 3.0), 1); ((4, 4.0), 1)]
        
        let! result = (semiOp :> IBinaryOperator<_,_,_>).EvalAsync leftData rightData
        
        let resultList = ZSet.toSeq result |> Seq.sortBy fst |> List.ofSeq
        
        Assert.AreEqual(2, resultList.Length)
        Assert.IsTrue(resultList |> List.exists (fun ((k, v), _) -> k = 1 && v = "a"))
        Assert.IsTrue(resultList |> List.exists (fun ((k, v), _) -> k = 3 && v = "c"))
        Assert.IsFalse(resultList |> List.exists (fun ((k, v), _) -> k = 2))
    }
    
    [<Test>]
    member _.``AntiJoin returns only left records without matching keys in right``() = task {
        let antiOp = antiJoin<int, string, float>()
        
        let leftData = ZSet.ofSeq [((1, "a"), 1); ((2, "b"), 1); ((3, "c"), 1)]
        let rightData = ZSet.ofSeq [((1, 1.0), 1); ((3, 3.0), 1); ((4, 4.0), 1)]
        
        let! result = (antiOp :> IBinaryOperator<_,_,_>).EvalAsync leftData rightData
        
        let resultList = ZSet.toSeq result |> Seq.sortBy fst |> List.ofSeq
        
        Assert.AreEqual(1, resultList.Length)
        Assert.IsTrue(resultList |> List.exists (fun ((k, v), _) -> k = 2 && v = "b"))
        Assert.IsFalse(resultList |> List.exists (fun ((k, v), _) -> k = 1))
        Assert.IsFalse(resultList |> List.exists (fun ((k, v), _) -> k = 3))
    }
    
    [<Test>]
    member _.``CrossJoin produces cartesian product``() = task {
        let crossOp = crossJoin<string, int>()
        
        let leftData = ZSet.ofSeq [("a", 1); ("b", 1)]
        let rightData = ZSet.ofSeq [(1, 1); (2, 1)]
        
        let! result = (crossOp :> IBinaryOperator<_,_,_>).EvalAsync leftData rightData
        
        let resultList = ZSet.toSeq result |> Seq.sortBy fst |> List.ofSeq
        
        Assert.AreEqual(4, resultList.Length)
        Assert.IsTrue(resultList |> List.exists (fun ((v1, v2), _) -> v1 = "a" && v2 = 1))
        Assert.IsTrue(resultList |> List.exists (fun ((v1, v2), _) -> v1 = "a" && v2 = 2))
        Assert.IsTrue(resultList |> List.exists (fun ((v1, v2), _) -> v1 = "b" && v2 = 1))
        Assert.IsTrue(resultList |> List.exists (fun ((v1, v2), _) -> v1 = "b" && v2 = 2))
    }
    
    [<Test>]
    member _.``Incremental LeftOuterJoin updates correctly``() = task {
        let leftOp = leftOuterJoin<int, string, float>()
        
        // Initial data
        let leftData1 = ZSet.ofSeq [((1, "a"), 1); ((2, "b"), 1)]
        let rightData1 = ZSet.ofSeq [((1, 1.0), 1)]
        
        let! result1 = (leftOp :> IBinaryOperator<_,_,_>).EvalAsync leftData1 rightData1
        
        // Add more data
        let leftData2 = ZSet.ofSeq [((3, "c"), 1)]
        let rightData2 = ZSet.ofSeq [((2, 2.0), 1)]
        
        let! result2 = (leftOp :> IBinaryOperator<_,_,_>).EvalAsync leftData2 rightData2
        
        // result2 should contain incremental updates only
        let result2List = ZSet.toSeq result2 |> List.ofSeq
        
        // Should have new left record (3, "c", None) and updated match (2, "b", Some 2.0)
        Assert.IsTrue(result2List |> List.exists (fun ((k, v1, v2), _) -> k = 3 && v1 = "c" && v2 = None))
        Assert.IsTrue(result2List |> List.exists (fun ((k, v1, v2), _) -> k = 2 && v1 = "b" && v2 = Some 2.0))
    }
    
    [<Test>]
    member _.``Incremental AntiJoin handles negative weights correctly``() = task {
        let antiOp = antiJoin<int, string, float>()
        
        // Initial: left has keys 1,2,3; right has key 1
        let leftData1 = ZSet.ofSeq [((1, "a"), 1); ((2, "b"), 1); ((3, "c"), 1)]
        let rightData1 = ZSet.ofSeq [((1, 1.0), 1)]
        
        let! result1 = (antiOp :> IBinaryOperator<_,_,_>).EvalAsync leftData1 rightData1
        
        // result1 should have keys 2,3
        let result1List = ZSet.toSeq result1 |> List.ofSeq
        Assert.AreEqual(2, result1List.Length)
        
        // Now add key 2 to right
        let leftData2 = ZSet.empty<int * string>
        let rightData2 = ZSet.ofSeq [((2, 2.0), 1)]
        
        let! result2 = (antiOp :> IBinaryOperator<_,_,_>).EvalAsync leftData2 rightData2
        
        // result2 should contain negative weight for (2, "b") since it's no longer in anti-join
        let result2List = ZSet.toSeq result2 |> List.ofSeq
        Assert.IsTrue(result2List |> List.exists (fun ((k, v), w) -> k = 2 && v = "b" && w = -1))
    }
    
    [<Test>]
    member _.``CrossJoin with weights multiplies correctly``() = task {
        let crossOp = crossJoin<string, int>()
        
        let leftData = ZSet.ofSeq [("a", 2); ("b", 3)]
        let rightData = ZSet.ofSeq [(1, 1); (2, 2)]
        
        let! result = (crossOp :> IBinaryOperator<_,_,_>).EvalAsync leftData rightData
        
        let resultList = ZSet.toSeq result |> List.ofSeq
        
        // Weights should be multiplied
        Assert.IsTrue(resultList |> List.exists (fun ((v1, v2), w) -> v1 = "a" && v2 = 1 && w = 2))
        Assert.IsTrue(resultList |> List.exists (fun ((v1, v2), w) -> v1 = "a" && v2 = 2 && w = 4))
        Assert.IsTrue(resultList |> List.exists (fun ((v1, v2), w) -> v1 = "b" && v2 = 1 && w = 3))
        Assert.IsTrue(resultList |> List.exists (fun ((v1, v2), w) -> v1 = "b" && v2 = 2 && w = 6))
    }
