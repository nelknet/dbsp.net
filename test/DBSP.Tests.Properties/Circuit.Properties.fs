/// Property-based tests for circuit behavior and construction
/// Ensures circuit execution maintains mathematical properties
module DBSP.Tests.Properties.Circuit

open System
open System.Threading.Tasks
open NUnit.Framework
open FsCheck
open FsCheck.FSharp
open FsCheck.NUnit
open FsCheck.NUnit
open DBSP.Core.ZSet
open DBSP.Circuit
open DBSP.Operators.LinearOperators
open DBSP.Operators.Interfaces
open DBSP.Tests.Properties.Generators

/// Properties for circuit construction and execution
[<TestFixture>]
type CircuitConstructionProperties() =
    
    /// Test that simple circuits behave correctly with basic inputs
    [<FsCheck.NUnit.Property(Arbitrary = [| typeof<ZSetGenerators> |])>]
    member _.``Simple circuits process inputs correctly``(inputs: int list) =
        if List.isEmpty inputs then true
        else
            // Create a simple operator that doubles values
            let mapOp = MapOperator<ZSet<int>, ZSet<int>>(ZSet.mapKeys (fun x -> x * 2))
            let inputZSet = inputs |> List.map (fun x -> (x, 1)) |> ZSet.ofList
            let expectedResult = ZSet.mapKeys (fun x -> x * 2) inputZSet
            
            // Use the operator directly
            let actualResult = (mapOp :> IUnaryOperator<ZSet<int>, ZSet<int>>).EvalAsync(inputZSet) |> Async.AwaitTask |> Async.RunSynchronously
            actualResult = expectedResult

/// Properties for circuit execution consistency
[<TestFixture>]
type CircuitExecutionProperties() =
    
    /// Test that circuit execution is deterministic
    [<FsCheck.NUnit.Property(Arbitrary = [| typeof<ZSetGenerators> |])>]
    member _.``Circuit execution is deterministic``(inputs: int list) =
        if inputs.Length > 10 then true // Skip very large inputs for performance
        else
            let inputZSet = inputs |> List.map (fun x -> (x, 1)) |> ZSet.ofList
            
            // Create two identical operators
            let mapOp1 = MapOperator<ZSet<int>, ZSet<int>>(ZSet.mapKeys (fun x -> abs x % 100))
            let mapOp2 = MapOperator<ZSet<int>, ZSet<int>>(ZSet.mapKeys (fun x -> abs x % 100))
            
            // Process same input through both operators
            let result1 = (mapOp1 :> IUnaryOperator<ZSet<int>, ZSet<int>>).EvalAsync(inputZSet) |> Async.AwaitTask |> Async.RunSynchronously
            let result2 = (mapOp2 :> IUnaryOperator<ZSet<int>, ZSet<int>>).EvalAsync(inputZSet) |> Async.AwaitTask |> Async.RunSynchronously
            
            result1 = result2

/// Properties for error handling and edge cases
[<TestFixture>]
type CircuitEdgeCaseProperties() =
    
    /// Test circuit behavior with empty inputs
    [<Test>]
    member _.``Circuits handle empty inputs correctly``() =
        let mapOp = MapOperator<ZSet<int>, ZSet<int>>(ZSet.mapKeys (fun x -> x * 2))
        let result = (mapOp :> IUnaryOperator<ZSet<int>, ZSet<int>>).EvalAsync(ZSet.empty<int>) |> Async.AwaitTask |> Async.RunSynchronously
        Assert.That(result, Is.EqualTo ZSet.empty<int>)
    
    /// Test circuit behavior with zero-weight elements
    [<FsCheck.NUnit.Property(Arbitrary = [| typeof<ZSetGenerators> |])>]
    member _.``Circuits filter out zero weights correctly``(key: int) =
        let mapOp = MapOperator<ZSet<int>, ZSet<int>>(ZSet.mapKeys id)
        
        // Send ZSet with zero weight
        let zeroWeightZSet = ZSet.singleton key 0
        let result = (mapOp :> IUnaryOperator<ZSet<int>, ZSet<int>>).EvalAsync(zeroWeightZSet) |> Async.AwaitTask |> Async.RunSynchronously
        result.IsEmpty
