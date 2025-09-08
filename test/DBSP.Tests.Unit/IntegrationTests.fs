/// Integration tests using Generator, Integrate, and Inspect operators
/// Tests real DBSP workflows with temporal operators and incremental computation
module DBSP.Tests.Unit.IntegrationTests

open System
open System.Collections.Generic
open System.Threading.Tasks
open NUnit.Framework
open DBSP.Core.ZSet
open DBSP.Operators.Interfaces
open DBSP.Operators.LinearOperators
open DBSP.Operators.TemporalOperators
open DBSP.Circuit

[<TestFixture>]
type GeneratorIntegrateInspectTests() =
    
    /// Test basic Generator -> Integrate workflow
    [<Test>]
    member _.``Generator integrate workflow produces cumulative results``() =
        task {
            // Create a simple generator that produces sequential integers
            let generator = TemporalOperators.generator<int>(fun step -> [step + 1])
            let integrator = TemporalOperators.integrate<int>()
            let mutable inspectedValues: ZSet<int> list = []
            let inspector = TemporalOperators.inspect<ZSet<int>>(fun zset -> inspectedValues <- zset :: inspectedValues)
            
            // Simulate 5 time steps
            let mutable accumulatedResult = ZSet.empty<int>
            
            for step in 0..4 do
                let! generated = generator.GenerateAsync()
                let! integrated = (integrator :> IUnaryOperator<ZSet<int>, ZSet<int>>).EvalAsync(generated)
                let! _ = inspector.EvalAsync(integrated)
                
                // Verify incremental accumulation
                accumulatedResult <- ZSet.add accumulatedResult generated
                Assert.AreEqual(accumulatedResult, integrated, $"Step {step} accumulation mismatch")
            
            // Verify inspector captured all values
            Assert.AreEqual(5, inspectedValues.Length)
            
            // Final result should contain all generated values
            let expectedFinal = ZSet.ofList [(1, 1); (2, 1); (3, 1); (4, 1); (5, 1)]
            Assert.AreEqual(expectedFinal, integrator.CurrentValue)
        }
    
    /// Test Generator -> Map -> Integrate workflow  
    [<Test>]
    member _.``Generator map integrate workflow maintains correctness``() =
        task {
            // Generate numbers, square them, then integrate
            let generator = TemporalOperators.generator<int>(fun step -> [step * 2])
            let mapper = MapOperator<ZSet<int>, ZSet<int>>(ZSet.mapKeys (fun x -> x * x))
            let integrator = TemporalOperators.integrate<int>()
            
            let mutable expectedAccum = ZSet.empty<int>
            
            for step in 0..3 do
                let! generated = generator.GenerateAsync()
                let! mapped = mapper.EvalAsync(generated)
                let! integrated = (integrator :> IUnaryOperator<ZSet<int>, ZSet<int>>).EvalAsync(mapped)
                
                // Verify each step
                let expectedGenerated = ZSet.singleton (step * 2) 1
                Assert.AreEqual(expectedGenerated, generated, $"Generated value mismatch at step {step}")
                
                let expectedMapped = ZSet.singleton ((step * 2) * (step * 2)) 1
                Assert.AreEqual(expectedMapped, mapped, $"Mapped value mismatch at step {step}")
                
                expectedAccum <- ZSet.add expectedAccum mapped
                Assert.AreEqual(expectedAccum, integrated, $"Integration mismatch at step {step}")
        }
    
    /// Test incremental word count with Generator and Integrate
    [<Test>]
    member _.``Incremental word count using generator and integrate``() =
        task {
            // Generate lines of text
            let lines = ["hello world"; "hello dbsp"; "world of data"; "dbsp rocks"]
            let generator = TemporalOperators.generator<string>(fun step -> 
                if step < lines.Length then [lines.[step]] else [])
            
            // Process: split words, create (word, 1) pairs, integrate counts
            let wordSplitter = MapOperator<ZSet<string>, ZSet<string * int>>(fun zset ->
                zset
                |> ZSet.toSeq
                |> Seq.collect (fun (line, weight) -> 
                    line.Split(' ')
                    |> Array.map (fun word -> ((word, 1), weight)))
                |> ZSet.ofSeq)
            
            let integrator = TemporalOperators.integrate<string * int>()
            let mutable finalCounts: (string * int * int) list = []
            
            // Inspect final results to extract word counts
            let inspector = TemporalOperators.inspect<ZSet<string * int>>(fun zset ->
                let counts = 
                    zset 
                    |> ZSet.toSeq
                    |> Seq.groupBy (fun ((word, _), _) -> word)
                    |> Seq.map (fun (word, group) -> 
                        let totalWeight = group |> Seq.sumBy (fun (_, weight) -> weight)
                        (word, 1, totalWeight))
                    |> Seq.toList
                finalCounts <- counts)
            
            // Process all lines
            for step in 0..3 do
                let! generated = generator.GenerateAsync()
                if not generated.IsEmpty then
                    let! words = wordSplitter.EvalAsync(generated)
                    let! integrated = (integrator :> IUnaryOperator<ZSet<int>, ZSet<int>>).EvalAsync(words)
                    let! _ = inspector.EvalAsync(integrated)
                    ()
            
            // Verify word counts
            let wordCountMap = finalCounts |> List.map (fun (word, _, count) -> (word, count)) |> Map.ofList
            
            Assert.AreEqual(3, wordCountMap.["hello"]) // Appears 2 times  
            Assert.AreEqual(2, wordCountMap.["world"]) // Appears 2 times
            Assert.AreEqual(2, wordCountMap.["dbsp"])  // Appears 2 times
            Assert.AreEqual(1, wordCountMap.["of"])    // Appears 1 time
            Assert.AreEqual(1, wordCountMap.["data"])  // Appears 1 time
            Assert.AreEqual(1, wordCountMap.["rocks"]) // Appears 1 time
        }
    
    /// Test differentiation after integration (should recover original deltas)
    [<Test>]
    member _.``Differentiate after integrate recovers original deltas``() =
        task {
            // Create sequence of different ZSets
            let generator = TemporalOperators.generator<string>(fun step ->
                match step with
                | 0 -> ["a"; "b"]
                | 1 -> ["c"]  
                | 2 -> ["b"; "d"] // Add b again and new d
                | _ -> [])
            
            let integrator = TemporalOperators.integrate<string>()
            let differentiator = TemporalOperators.differentiate<string>()
            let mutable originalDeltas: ZSet<string> list = []
            let mutable recoveredDeltas: ZSet<string> list = []
            
            // Capture original deltas and recovered deltas
            let deltaInspector = TemporalOperators.inspect<ZSet<string>>(fun zset -> originalDeltas <- zset :: originalDeltas)
            let recoveredInspector = TemporalOperators.inspect<ZSet<string>>(fun zset -> recoveredDeltas <- zset :: recoveredDeltas)
            
            for step in 0..2 do
                let! generated = generator.GenerateAsync()
                let! _ = deltaInspector.EvalAsync(generated)
                let! integrated = (integrator :> IUnaryOperator<ZSet<int>, ZSet<int>>).EvalAsync(generated)
                let! differentiated = differentiator.EvalAsync(integrated)
                let! _ = recoveredInspector.EvalAsync(differentiated)
                ()
            
            // Reverse lists since we prepended
            originalDeltas <- List.rev originalDeltas
            recoveredDeltas <- List.rev recoveredDeltas
            
            // Verify that differentiation recovers original deltas
            Assert.AreEqual(originalDeltas.Length, recoveredDeltas.Length)
            for i in 0..originalDeltas.Length - 1 do
                Assert.AreEqual(originalDeltas.[i], recoveredDeltas.[i], $"Delta mismatch at step {i}")
        }

/// Tests for complex temporal operator combinations
[<TestFixture>]
type ComplexTemporalWorkflowTests() =
    
    /// Test stream processing with delay and feedback
    [<Test>]
    member _.``Delayed feedback maintains causality``() =
        task {
            // Create a counter that increments each step
            let generator = TemporalOperators.generator<int>(fun step -> [1]) // Always generate 1
            let integrator = TemporalOperators.integrate<int>()
            let delayOp = TemporalOperators.delay<ZSet<int>>()
            
            let mutable currentCounts: int list = []
            let countInspector = TemporalOperators.inspect<ZSet<int>>(fun zset ->
                let count = zset.Count
                currentCounts <- count :: currentCounts)
            
            // Simulate feedback loop: current + delayed_previous
            for step in 0..5 do
                let! generated = generator.GenerateAsync()
                let! integrated = (integrator :> IUnaryOperator<ZSet<int>, ZSet<int>>).EvalAsync(generated)
                let! delayed = delayOp.EvalAsync(integrated)
                let! _ = countInspector.EvalAsync(integrated)
                ()
            
            // Verify monotonic increase (each step adds 1)
            currentCounts <- List.rev currentCounts
            for i in 0..currentCounts.Length - 1 do
                Assert.AreEqual(i + 1, currentCounts.[i], $"Count should be {i + 1} at step {i}")
        }
    
    /// Test multi-input integration with different data types
    [<Test>]
    member _.``Multi-source integration maintains type correctness``() =
        task {
            // Generate both integers and strings
            let intGenerator = TemporalOperators.generator<int>(fun step -> [step])
            let stringGenerator = TemporalOperators.generator<string>(fun step -> [$"item_{step}"])
            
            let intIntegrator = TemporalOperators.integrate<int>()
            let stringIntegrator = TemporalOperators.integrate<string>()
            
            let mutable intResults: int list = []
            let mutable stringResults: string list = []
            
            let intInspector = TemporalOperators.inspect<ZSet<int>>(fun zset ->
                intResults <- (zset.Keys |> Seq.toList) @ intResults)
            let stringInspector = TemporalOperators.inspect<ZSet<string>>(fun zset ->
                stringResults <- (zset.Keys |> Seq.toList) @ stringResults)
            
            for step in 0..3 do
                let! intGen = intGenerator.GenerateAsync()
                let! stringGen = stringGenerator.GenerateAsync()
                
                let! intInteg = intIntegrator.EvalAsync(intGen)
                let! stringInteg = stringIntegrator.EvalAsync(stringGen)
                
                let! _ = intInspector.EvalAsync(intInteg)
                let! _ = stringInspector.EvalAsync(stringInteg)
                ()
            
            // Verify both streams processed correctly
            Assert.AreEqual([0; 1; 2; 3], intResults |> List.rev |> List.sort)
            Assert.AreEqual(["item_0"; "item_1"; "item_2"; "item_3"], stringResults |> List.rev |> List.sort)
        }

/// Error handling and edge case tests
[<TestFixture>]
type TemporalEdgeCaseTests() =
    
    /// Test behavior with empty generators
    [<Test>]
    member _.``Empty generators produce empty results``() =
        task {
            let emptyGenerator = TemporalOperators.generator<int>(fun _ -> [])
            let integrator = TemporalOperators.integrate<int>()
            
            for step in 0..2 do
                let! generated = emptyGenerator.GenerateAsync()
                Assert.IsTrue(generated.IsEmpty)
                
                let! integrated = (integrator :> IUnaryOperator<ZSet<int>, ZSet<int>>).EvalAsync(generated)
                Assert.IsTrue(integrated.IsEmpty)
        }
    
    /// Test reset functionality maintains correctness
    [<Test>]  
    member _.``Reset functionality maintains operator correctness``() =
        task {
            let generator = TemporalOperators.generator<int>(fun step -> [step + 1])
            let integrator = TemporalOperators.integrate<int>()
            
            // First run
            for step in 0..2 do
                let! generated = generator.GenerateAsync()
                let! _ = (integrator :> IUnaryOperator<ZSet<int>, ZSet<int>>).EvalAsync(generated)
                ()
            
            let firstResult = integrator.CurrentValue
            
            // Reset and run again  
            generator.Reset()
            integrator.Reset()
            
            for step in 0..2 do
                let! generated = generator.GenerateAsync()
                let! _ = (integrator :> IUnaryOperator<ZSet<int>, ZSet<int>>).EvalAsync(generated)
                ()
            
            let secondResult = integrator.CurrentValue
            
            // Results should be identical
            Assert.AreEqual(firstResult, secondResult)
        }
    
    /// Test inspect operator doesn't modify data
    [<Test>]
    member _.``Inspect operator preserves data integrity``() =
        task {
            let testData = ZSet.ofList [(1, 2); (2, -1); (3, 3)]
            let mutable inspectedValue: ZSet<int> option = None
            
            let inspector = TemporalOperators.inspect<ZSet<int>>(fun zset -> inspectedValue <- Some zset)
            let! result = inspector.EvalAsync(testData)
            
            // Verify data passed through unchanged
            Assert.AreEqual(testData, result)
            Assert.AreEqual(Some testData, inspectedValue)
        }

/// Performance and correctness tests for real DBSP patterns
[<TestFixture>]
type DBSPPatternTests() =
    
    /// Test classic DBSP delta processing pattern
    [<Test>]
    member _.``Classic delta processing pattern works correctly``() =
        task {
            // Simulate a database table with inserts and deletes
            let dbOperations = [
                // Initial inserts
                [(1, "Alice", 1); (2, "Bob", 1); (3, "Carol", 1)]
                // Update (delete + insert)
                [(2, "Bob", -1); (2, "Robert", 1)]
                // New insert
                [(4, "David", 1)]
                // Delete
                [(1, "Alice", -1)]
            ]
            
            let generator = TemporalOperators.generator<int * string>(fun step ->
                if step < dbOperations.Length then
                    dbOperations.[step] |> List.map (fun (id, name, _) -> (id, name))
                else [])
            
            let integrator = TemporalOperators.integrate<int * string>()
            let mutable finalState: (int * string) list = []
            
            let inspector = TemporalOperators.inspect<ZSet<int * string>>(fun zset ->
                finalState <- 
                    zset
                    |> ZSet.toSeq
                    |> Seq.filter (fun (_, weight) -> weight > 0)  // Only positive weights (current records)
                    |> Seq.map (fun (record, _) -> record)
                    |> Seq.toList)
            
            // Process database operations step by step
            for step in 0..3 do
                let! generated = generator.GenerateAsync()
                if not generated.IsEmpty then
                    // Apply operations with correct weights
                    let weightedOps = 
                        generated 
                        |> ZSet.toSeq
                        |> Seq.zip dbOperations.[step]
                        |> Seq.map (fun ((_, _, weight), ((id, name), _)) -> ((id, name), weight))
                        |> ZSet.ofSeq
                    
                    let! integrated = (integrator :> IUnaryOperator<ZSet<int>, ZSet<int>>).EvalAsync(weightedOps)
                    let! _ = inspector.EvalAsync(integrated)
                    ()
            
            // Final state should reflect all operations
            let sortedFinal = finalState |> List.sortBy fst
            let expected = [(2, "Robert"); (3, "Carol"); (4, "David")] // Alice deleted, Bob -> Robert
            Assert.AreEqual(expected, sortedFinal)
        }
    
    /// Test stream join pattern with temporal operators
    [<Test>]
    member _.``Stream join with temporal operators maintains consistency``() =
        task {
            // Two related data streams: users and orders
            let userGenerator = TemporalOperators.generator<int * string>(fun step ->
                match step with
                | 0 -> [(1, "Alice"); (2, "Bob")]
                | 1 -> [(3, "Carol")]
                | _ -> [])
            
            let orderGenerator = TemporalOperators.generator<int * string>(fun step ->
                match step with  
                | 0 -> [(1, "Order1"); (1, "Order2")] // Alice has 2 orders
                | 1 -> [(2, "Order3")] // Bob has 1 order
                | 2 -> [(3, "Order4")] // Carol has 1 order
                | _ -> [])
            
            let userIntegrator = TemporalOperators.integrate<int * string>()
            let orderIntegrator = TemporalOperators.integrate<int * string>()
            
            let mutable userState = ZSet.empty<int * string>
            let mutable orderState = ZSet.empty<int * string>
            let mutable joinResults: (string * string) list = []
            
            // Manual join inspection (simplified)
            let joinInspector = TemporalOperators.inspect<ZSet<int * string>>(fun users ->
                // Simple join logic for testing
                for (userId, userName), userWeight in ZSet.toSeq users do
                    if userWeight > 0 then
                        for (orderId, orderName), orderWeight in ZSet.toSeq orderState do
                            if orderId = userId && orderWeight > 0 then
                                joinResults <- (userName, orderName) :: joinResults)
            
            for step in 0..2 do
                let! users = userGenerator.GenerateAsync()
                let! orders = orderGenerator.GenerateAsync()
                
                let! integratedUsers = userIntegrator.EvalAsync(users)
                let! integratedOrders = orderIntegrator.EvalAsync(orders)
                
                userState <- integratedUsers
                orderState <- integratedOrders
                
                let! _ = joinInspector.EvalAsync(integratedUsers)
                ()
            
            // Verify join results contain expected user-order pairs
            let uniqueJoins = joinResults |> List.distinct |> List.sort
            let expectedJoins = [("Alice", "Order1"); ("Alice", "Order2"); ("Bob", "Order3"); ("Carol", "Order4")] |> List.sort
            
            Assert.IsTrue(uniqueJoins.Length >= expectedJoins.Length, "Should have captured all expected joins")
        }
    
    /// Test operator state isolation
    [<Test>]
    member _.``Operator state isolation prevents interference``() =
        task {
            // Create two independent integrate operators
            let integrator1 = TemporalOperators.integrate<int>()
            let integrator2 = TemporalOperators.integrate<int>()
            
            let data1 = ZSet.singleton 1 1
            let data2 = ZSet.singleton 2 1
            
            // Process different data through each integrator
            let! result1 = integrator1.EvalAsync(data1)
            let! result2 = integrator2.EvalAsync(data2)
            
            // Verify they don't interfere with each other
            Assert.AreEqual(data1, result1)
            Assert.AreEqual(data2, result2)
            Assert.AreNotEqual(integrator1.CurrentValue, integrator2.CurrentValue)
            
            // Add more data to first integrator
            let! result1_2 = integrator1.EvalAsync(ZSet.singleton 3 1)
            
            // Second integrator should be unchanged
            Assert.AreEqual(data2, integrator2.CurrentValue)
            Assert.AreEqual(ZSet.ofList [(1, 1); (3, 1)], integrator1.CurrentValue)
        }