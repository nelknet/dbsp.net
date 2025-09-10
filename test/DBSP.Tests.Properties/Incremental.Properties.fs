/// Property-based tests for incremental computation correctness
/// Ensures that incremental operators produce the same results as batch computation
module DBSP.Tests.Properties.Incremental

open System
open System.Threading.Tasks
open NUnit.Framework
open FsCheck
open FsCheck.FSharp
open FsCheck.NUnit
open FsCheck.NUnit
open DBSP.Core.ZSet
open DBSP.Operators.Interfaces
open DBSP.Operators.LinearOperators
open DBSP.Tests.Properties.Generators

/// Batch computation reference implementations for testing
module BatchReference =
    
    /// Batch map operation - applies transformation to accumulated ZSet
    let batchMap<'T, 'U when 'T: comparison and 'U: comparison> 
        (transform: 'T -> 'U) (deltas: ZSet<'T> list) : ZSet<'U> =
        deltas
        |> List.fold ZSet.add ZSet.empty
        |> ZSet.mapKeys transform
    
    /// Batch filter operation - applies predicate to accumulated ZSet
    let batchFilter<'T when 'T: comparison> 
        (predicate: 'T -> bool) (deltas: ZSet<'T> list) : ZSet<'T> =
        deltas
        |> List.fold ZSet.add ZSet.empty  
        |> ZSet.filter predicate

/// Incremental computation implementations using actual operators
module IncrementalComputation =
    
    /// Incremental map using MapOperator
    let incrementalMap<'T, 'U when 'T: comparison and 'U: comparison> 
        (transform: 'T -> 'U) (deltas: ZSet<'T> list) : Task<ZSet<'U>> =
        task {
            let mapOp = MapOperator<ZSet<'T>, ZSet<'U>>(ZSet.mapKeys transform) :> IUnaryOperator<ZSet<'T>, ZSet<'U>>
            let mutable result = ZSet.empty<'U>
            
            for delta in deltas do
                let! deltaResult = mapOp.EvalAsync(delta)
                result <- ZSet.add result deltaResult
                
            return result
        }
    
    /// Incremental filter using FilterOperator
    let incrementalFilter<'T when 'T: comparison>
        (predicate: 'T -> bool) (deltas: ZSet<'T> list) : Task<ZSet<'T>> =
        task {
            let filterOp = ZSetFilterOperator<'T>(predicate) :> IUnaryOperator<ZSet<'T>, ZSet<'T>>
            let mutable result = ZSet.empty<'T>
            
            for delta in deltas do
                let! deltaResult = filterOp.EvalAsync(delta)
                result <- ZSet.add result deltaResult
                
            return result
        }

/// Core incremental correctness properties
[<TestFixture>]
type IncrementalCorrectnessProperties() =
    
    /// Test that incremental map produces same result as batch map
    [<FsCheck.NUnit.Property(Arbitrary = [| typeof<ZSetGenerators> |])>]
    member _.``Incremental map equals batch map``(deltas: ZSet<int> list) =
        if List.isEmpty deltas then true
        else
            let transform x = x * 2 + 1 // Simple transformation
            let batchResult = BatchReference.batchMap transform deltas
            let incrementalResult = IncrementalComputation.incrementalMap transform deltas |> Async.AwaitTask |> Async.RunSynchronously
            batchResult = incrementalResult
    
    /// Test that incremental filter produces same result as batch filter
    [<FsCheck.NUnit.Property(Arbitrary = [| typeof<ZSetGenerators> |])>]
    member _.``Incremental filter equals batch filter``(deltas: ZSet<int> list) =
        if List.isEmpty deltas then true
        else
            let predicate x = x % 2 = 0 // Even numbers only
            let batchResult = BatchReference.batchFilter predicate deltas
            let incrementalResult = IncrementalComputation.incrementalFilter predicate deltas |> Async.AwaitTask |> Async.RunSynchronously
            batchResult = incrementalResult

/// Delta processing properties - test that the sequence of deltas doesn't affect final result
[<TestFixture>]
type DeltaProcessingProperties() =
    
    /// Test that order of independent deltas doesn't matter for commutative operations
    [<FsCheck.NUnit.Property(Arbitrary = [| typeof<ZSetGenerators> |])>]
    member _.``Delta order independence for commutative operations``(deltas: ZSet<int> list) =
        if deltas.Length < 2 then true
        else
            let transform x = x * 2
            let shuffledDeltas = deltas |> List.rev // Simple reordering
            
            let result1 = BatchReference.batchMap transform deltas
            let result2 = BatchReference.batchMap transform shuffledDeltas
            result1 = result2
    
    /// Test that splitting deltas maintains correctness
    [<FsCheck.NUnit.Property(Arbitrary = [| typeof<ZSetGenerators> |])>]
    member _.``Delta splitting maintains correctness``(bigDelta: ZSet<int>) =
        if bigDelta.IsEmpty then true
        else
            // Split big delta into smaller pieces
            let pairs = ZSet.toSeq bigDelta |> Seq.toList
            if pairs.Length <= 1 then true
            else
                let split1 = pairs |> List.take (pairs.Length / 2) |> ZSet.ofList
                let split2 = pairs |> List.skip (pairs.Length / 2) |> ZSet.ofList
                
                let directResult = ZSet.mapKeys (fun x -> x + 1) bigDelta
                let splitResult = 
                    [split1; split2]
                    |> BatchReference.batchMap (fun x -> x + 1)
                
                directResult = splitResult

/// Edge case properties for incremental computation
[<TestFixture>]
type IncrementalEdgeCaseProperties() =
    
    /// Test empty delta sequences
    [<Test>]
    member _.``Empty deltas produce empty results``() =
        let emptyDeltas: ZSet<int> list = []
        let result = BatchReference.batchMap (fun x -> x + 1) emptyDeltas
        Assert.That(result, Is.EqualTo ZSet.empty<int>)
    
    /// Test single delta correctness
    [<FsCheck.NUnit.Property(Arbitrary = [| typeof<ZSetGenerators> |])>]
    member _.``Single delta processing is correct``(delta: ZSet<int>) =
        let transform x = x * 3
        let batchResult = BatchReference.batchMap transform [delta]
        let directResult = ZSet.mapKeys transform delta
        batchResult = directResult
    
    /// Test that zero-weight elements don't affect results
    [<FsCheck.NUnit.Property(Arbitrary = [| typeof<ZSetGenerators> |])>]
    member _.``Zero weight elements are ignored``(key: int) =
        let deltaWithZero = ZSet.singleton key 0
        let deltaWithoutZero = ZSet.empty<int>
        
        let result1 = BatchReference.batchMap (fun x -> x + 1) [deltaWithZero]
        let result2 = BatchReference.batchMap (fun x -> x + 1) [deltaWithoutZero]
        result1 = result2

/// Memory and performance properties
[<TestFixture>]
type IncrementalPerformanceProperties() =
    
    /// Test that incremental processing handles large weight magnitudes
    [<FsCheck.NUnit.Property(Arbitrary = [| typeof<ZSetGenerators> |])>]
    member _.``Large weights are handled correctly``(key: int) =
        let largePositive = ZSet.singleton key 10000
        let largeNegative = ZSet.singleton key (-10000)
        let deltas = [largePositive; largeNegative]
        
        let result = BatchReference.batchMap (fun x -> x) deltas
        result = ZSet.empty<int> // Should cancel out
    
    /// Test that repeated operations maintain consistency
    [<Property(Arbitrary = [| typeof<ZSetGenerators> |])>]
    member _.``Repeated operations are consistent``(delta: ZSet<int>) =
        if delta.IsEmpty then true
        else
            let transform x = x % 100 // Keep values bounded
            let repeated = [delta; delta; delta]
            let single = [ZSet.scalarMultiply 3 delta]
            
            let result1 = BatchReference.batchMap transform repeated
            let result2 = BatchReference.batchMap transform single
            result1 = result2
