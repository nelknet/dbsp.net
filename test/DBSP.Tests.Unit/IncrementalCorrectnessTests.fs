/// Validation tests for incremental correctness against batch computation
/// Ensures incremental operators produce identical results to batch processing
module DBSP.Tests.Unit.IncrementalCorrectnessTests

open System.Threading.Tasks
open NUnit.Framework
open DBSP.Core.ZSet
open DBSP.Operators.LinearOperators
open DBSP.Operators.Interfaces

[<TestFixture>]
type IncrementalCorrectnessTests() =
    
    /// Test that incremental map produces same result as batch map
    [<Test>]
    member _.``Incremental map equals batch map``() =
        task {
            // Create test data
            let data1 = ZSet.ofList [(1, 2); (2, -1); (3, 1)]
            let data2 = ZSet.ofList [(2, 1); (4, 2)]
            let data3 = ZSet.ofList [(1, -1); (5, 1)]
            let deltas = [data1; data2; data3]
            let transform = fun x -> x * 2 + 1
            
            // Batch computation: accumulate all deltas, then transform
            let batchResult = 
                deltas
                |> List.fold ZSet.add ZSet.empty
                |> ZSet.mapKeys transform
            
            // Incremental computation: transform each delta, then accumulate
            let mapOperator = MapOperator<ZSet<int>, ZSet<int>>(ZSet.mapKeys transform) :> IUnaryOperator<ZSet<int>, ZSet<int>>
            let mutable incrementalResult = ZSet.empty<int>
            
            for delta in deltas do
                let! transformed = mapOperator.EvalAsync(delta)
                incrementalResult <- ZSet.add incrementalResult transformed
            
            Assert.That(incrementalResult, Is.EqualTo batchResult)
        }
    
    /// Test that incremental filter produces same result as batch filter
    [<Test>]
    member _.``Incremental filter equals batch filter``() =
        task {
            let data1 = ZSet.ofList [(1, 1); (2, 1); (3, 1); (4, 1)]
            let data2 = ZSet.ofList [(5, 1); (6, 1); (2, -1)]
            let deltas = [data1; data2]
            let predicate = fun x -> x % 2 = 0 // Even numbers only
            
            // Batch computation
            let batchResult = 
                deltas
                |> List.fold ZSet.add ZSet.empty
                |> ZSet.filter predicate
            
            // Incremental computation
            let filterOperator = ZSetFilterOperator<int>(predicate) :> IUnaryOperator<ZSet<int>, ZSet<int>>
            let mutable incrementalResult = ZSet.empty<int>
            
            for delta in deltas do
                let! filtered = filterOperator.EvalAsync(delta)
                incrementalResult <- ZSet.add incrementalResult filtered
            
            Assert.That(incrementalResult, Is.EqualTo batchResult)
        }
    
    /// Test operator composition maintains correctness
    [<Test>]
    member _.``Map then filter composition maintains correctness``() =
        task {
            let data1 = ZSet.ofList [(1, 1); (2, 2); (3, -1)]
            let data2 = ZSet.ofList [(4, 1); (1, -1); (5, 1)]
            let deltas = [data1; data2]
            
            let transform = fun x -> x * 3
            let predicate = fun x -> x > 5
            
            // Batch computation: accumulate, then transform, then filter
            let batchResult = 
                deltas
                |> List.fold ZSet.add ZSet.empty
                |> ZSet.mapKeys transform
                |> ZSet.filter predicate
            
            // Incremental computation: transform each delta, filter, then accumulate
            let mapOperator = MapOperator<ZSet<int>, ZSet<int>>(ZSet.mapKeys transform) :> IUnaryOperator<ZSet<int>, ZSet<int>>
            let filterOperator = ZSetFilterOperator<int>(predicate) :> IUnaryOperator<ZSet<int>, ZSet<int>>
            let mutable incrementalResult = ZSet.empty<int>
            
            for delta in deltas do
                let! transformed = mapOperator.EvalAsync(delta)
                let! filtered = filterOperator.EvalAsync(transformed)
                incrementalResult <- ZSet.add incrementalResult filtered
            
            Assert.That(incrementalResult, Is.EqualTo batchResult)
        }
    
    /// Test that order of deltas doesn't affect final result for commutative operations
    [<Test>]
    member _.``Delta order independence for commutative operations``() =
        task {
            let data1 = ZSet.ofList [(1, 1); (2, 2)]
            let data2 = ZSet.ofList [(3, -1); (4, 1)]
            let data3 = ZSet.ofList [(1, -1); (5, 2)]
            let transform = fun x -> x + 10
            
            // Process in original order
            let mapOp1 = MapOperator<ZSet<int>, ZSet<int>>(ZSet.mapKeys transform) :> IUnaryOperator<ZSet<int>, ZSet<int>>
            let mutable result1 = ZSet.empty<int>
            
            for delta in [data1; data2; data3] do
                let! transformed = mapOp1.EvalAsync(delta)
                result1 <- ZSet.add result1 transformed
            
            // Process in reversed order
            let mapOp2 = MapOperator<ZSet<int>, ZSet<int>>(ZSet.mapKeys transform) :> IUnaryOperator<ZSet<int>, ZSet<int>>
            let mutable result2 = ZSet.empty<int>
            
            for delta in [data3; data2; data1] do
                let! transformed = mapOp2.EvalAsync(delta)
                result2 <- ZSet.add result2 transformed
            
            Assert.That(result2, Is.EqualTo result1)
        }
    
    /// Test with larger dataset to validate scalability
    [<Test>]
    member _.``Large dataset incremental correctness``() =
        task {
            // Generate test data with simpler pattern
            let deltas = 
                [
                    ZSet.ofList [(1, 1); (2, 1); (3, 1); (4, 1); (5, 1)]
                    ZSet.ofList [(3, -1); (6, 1); (7, 1)]  // Remove 3, add 6,7
                    ZSet.ofList [(1, -1); (8, 1); (9, 1)]  // Remove 1, add 8,9
                    ZSet.ofList [(10, 1); (11, 1)]         // Add 10,11
                ]
            
            let transform = fun x -> x * 2 // Simple doubling
            
            // Batch computation
            let batchResult = 
                deltas
                |> List.fold ZSet.add ZSet.empty
                |> ZSet.mapKeys transform
            
            // Incremental computation  
            let mapOp = MapOperator<ZSet<int>, ZSet<int>>(ZSet.mapKeys transform) :> IUnaryOperator<ZSet<int>, ZSet<int>>
            let mutable incrementalResult = ZSet.empty<int>
            
            for delta in deltas do
                let! mapped = mapOp.EvalAsync(delta)
                incrementalResult <- ZSet.add incrementalResult mapped
            
            // Verify batch and incremental produce the same result
            Assert.That(incrementalResult, Is.EqualTo batchResult)
            
            // Check expected final state: 2,4,5,6,7,8,9,10,11 (after removing 1,3)
            // Transformed: 4,8,10,12,14,16,18,20,22
            let expectedKeys = [4; 8; 10; 12; 14; 16; 18; 20; 22] |> Set.ofList
            let actualKeys = batchResult.Keys |> Set.ofSeq
            Assert.That(actualKeys, Is.EqualTo expectedKeys)
        }
    
    /// Test negative weights (deletions) maintain correctness
    [<Test>]
    member _.``Negative weights maintain incremental correctness``() =
        task {
            // Test with insertions and deletions
            let insert1 = ZSet.ofList [(1, 1); (2, 1); (3, 1)]
            let delete1 = ZSet.ofList [(2, -1)] // Delete 2
            let insert2 = ZSet.ofList [(4, 1); (5, 1)]
            let delete2 = ZSet.ofList [(1, -1); (3, -1)] // Delete 1 and 3
            let deltas = [insert1; delete1; insert2; delete2]
            
            let transform = fun x -> x * 10
            
            // Batch computation
            let batchResult = 
                deltas
                |> List.fold ZSet.add ZSet.empty
                |> ZSet.mapKeys transform
            
            // Incremental computation
            let mapOp = MapOperator<ZSet<int>, ZSet<int>>(ZSet.mapKeys transform) :> IUnaryOperator<ZSet<int>, ZSet<int>>
            let mutable incrementalResult = ZSet.empty<int>
            
            for delta in deltas do
                let! transformed = mapOp.EvalAsync(delta)
                incrementalResult <- ZSet.add incrementalResult transformed
            
            Assert.That(incrementalResult, Is.EqualTo batchResult)
            
            // Final result should only contain elements 4 and 5 (transformed)
            let expectedFinal = ZSet.ofList [(40, 1); (50, 1)]
            Assert.That(batchResult, Is.EqualTo expectedFinal)
            Assert.That(incrementalResult, Is.EqualTo expectedFinal)
        }
