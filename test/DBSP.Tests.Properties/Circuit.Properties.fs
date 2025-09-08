/// Property-based tests for circuit behavior and construction
/// Ensures circuit execution maintains mathematical properties
module DBSP.Tests.Properties.Circuit

open System
open System.Threading.Tasks
open NUnit.Framework
open FsCheck
open FsCheck.FSharp
open DBSP.Core.ZSet
open DBSP.Circuit
open DBSP.Operators.LinearOperators
open DBSP.Operators.Interfaces
open DBSP.Tests.Properties.Generators

/// Properties for circuit construction and execution
[<TestFixture>]
type CircuitConstructionProperties() =
    
    let config = Config.Quick.WithArbitrary([typeof<ZSetGenerators>])
    
    /// Test that simple circuits behave correctly with basic inputs
    [<Test>]
    member _.``Simple circuits process inputs correctly``() =
        let property (inputs: int list) =
            if List.isEmpty inputs then true
            else
                // Create a simple operator that doubles values
                let mapOp = MapOperator<ZSet<int>, ZSet<int>>(ZSet.mapKeys (fun x -> x * 2))
                let inputZSet = inputs |> List.map (fun x -> (x, 1)) |> ZSet.ofList
                let expectedResult = ZSet.mapKeys (fun x -> x * 2) inputZSet
                
                // Use the operator directly
                let actualResult = (mapOp :> IUnaryOperator<ZSet<int>, ZSet<int>>).EvalAsync(inputZSet) |> Async.AwaitTask |> Async.RunSynchronously
                actualResult = expectedResult
        Check.One(config, property)

/// Properties for circuit execution consistency
[<TestFixture>]
type CircuitExecutionProperties() =
    
    let config = Config.Quick.WithArbitrary([typeof<ZSetGenerators>])
    
    /// Test that circuit execution is deterministic
    [<Test>]
    member _.``Circuit execution is deterministic``() =
        let property (inputs: int list) =
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
        Check.One(config, property)

/// Properties for error handling and edge cases
[<TestFixture>]
type CircuitEdgeCaseProperties() =
    
    let config = Config.Quick.WithArbitrary([typeof<ZSetGenerators>])
    
    /// Test circuit behavior with empty inputs
    [<Test>]
    member _.``Circuits handle empty inputs correctly``() =
        let mapOp = MapOperator<ZSet<int>, ZSet<int>>(ZSet.mapKeys (fun x -> x * 2))
        let result = (mapOp :> IUnaryOperator<ZSet<int>, ZSet<int>>).EvalAsync(ZSet.empty<int>) |> Async.AwaitTask |> Async.RunSynchronously
        Assert.AreEqual(ZSet.empty<int>, result)
    
    /// Test circuit behavior with zero-weight elements
    [<Test>]
    member _.``Circuits filter out zero weights correctly``() =
        let property (key: int) =
            let mapOp = MapOperator<ZSet<int>, ZSet<int>>(ZSet.mapKeys id)
            
            // Send ZSet with zero weight
            let zeroWeightZSet = ZSet.singleton key 0
            let result = (mapOp :> IUnaryOperator<ZSet<int>, ZSet<int>>).EvalAsync(zeroWeightZSet) |> Async.AwaitTask |> Async.RunSynchronously
            result = ZSet.empty<int>
        Check.One(config, property)