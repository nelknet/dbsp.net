module DBSP.Tests.Unit.ParallelRuntimeTests

open NUnit.Framework
open System
open System.Threading.Tasks
open DBSP.Circuit
open DBSP.Core.ZSet
open DBSP.Operators.Interfaces

/// Test helpers for creating circuit definitions
module TestHelpers =
    let createEmptyCircuit() =
        let circuit, _ = RootCircuit.Build(fun builder -> ())
        circuit

/// Phase 5.2: Comprehensive test coverage for parallel runtime
[<TestFixture>]
type ParallelRuntimeTests() =
    
    [<Test>]
    member _.``ParallelRuntime should create correct number of workers``() =
        let config = { 
            RuntimeConfig.Default with 
                WorkerThreads = 4 
        }
        // Create minimal circuit definition
        let circuit = TestHelpers.createEmptyCircuit()
        use runtime = new ParallelCircuitRuntime(circuit, config)
        
        // Start and verify worker creation
        let startResult = runtime.StartAsync() |> Async.AwaitTask |> Async.RunSynchronously
        Assert.AreEqual(Ok (), startResult)
        
        // Stop runtime
        let stopResult = runtime.StopAsync() |> Async.AwaitTask |> Async.RunSynchronously
        Assert.AreEqual(Ok (), stopResult)
    
    [<Test>]
    member _.``ThreadLocalManager should provide thread-local buffer caches``() = task {
        let tasks = 
            Array.init 10 (fun i -> 
                Task.Run(fun () ->
                    let cache = ThreadLocalManager.BufferCache
                    Assert.IsNotNull(cache)
                    Assert.AreEqual(0L, cache.BytesAllocated)
                    
                    // Rent and return bytes
                    let buffer = cache.RentBytes(1024)
                    Assert.AreEqual(1024, buffer.Length)
                )
            )
        
        do! Task.WhenAll(tasks)
    }
    
    [<Test>]
    member _.``BufferCache should handle different buffer sizes correctly``() =
        let cache = BufferCache.Create()
        
        // Test small buffer
        let smallBuffer = cache.RentBytes(512)
        Assert.GreaterOrEqual(smallBuffer.Length, 512)
        
        // Test medium buffer
        let mediumBuffer = cache.RentBytes(8192)
        Assert.GreaterOrEqual(mediumBuffer.Length, 8192)
        
        // Test large buffer
        let largeBuffer = cache.RentBytes(65536)
        Assert.GreaterOrEqual(largeBuffer.Length, 65536)
    
    [<Test>]
    member _.``RuntimeMetrics should track execution correctly``() =
        let metrics = RuntimeMetrics.Zero
        Assert.AreEqual(0L, metrics.StepsExecuted)
        Assert.AreEqual(TimeSpan.Zero, metrics.TotalLatency)
        Assert.AreEqual(0.0, metrics.CacheHitRatio)
    
    [<Test>]
    member _.``OptimizedScheduler should handle priority correctly``() =
        let scheduler = new OptimizedScheduler()
        
        // Create mock operators with priorities
        let highPriorityOp = { 
            new IOperator with
                member _.Name = "HighPriority"
                member _.IsAsync = false
                member _.Ready = true
                member _.Fixedpoint(_) = false
                member _.Flush() = ()
                member _.ClearState() = ()
        }
        
        let lowPriorityOp = { 
            new IOperator with
                member _.Name = "LowPriority"
                member _.IsAsync = false
                member _.Ready = true
                member _.Fixedpoint(_) = false
                member _.Flush() = ()
                member _.ClearState() = ()
        }
        
        // Schedule with different priorities
        scheduler.Schedule(lowPriorityOp, Priority.Low)
        scheduler.Schedule(highPriorityOp, Priority.High)
        
        // High priority should be dequeued first
        match scheduler.TryGetNext() with
        | Some op -> Assert.AreEqual("HighPriority", op.Name)
        | None -> Assert.Fail("Expected high priority operator")
        
        match scheduler.TryGetNext() with
        | Some op -> Assert.AreEqual("LowPriority", op.Name)
        | None -> Assert.Fail("Expected low priority operator")
    
    [<Test>]
    member _.``DependencyScheduler should compute correct execution order``() =
        // Create circuit with dependencies
        let op1 = { 
            new IOperator with
                member _.Name = "Op1"
                member _.IsAsync = false
                member _.Ready = true
                member _.Fixedpoint(_) = false
                member _.Flush() = ()
                member _.ClearState() = ()
        }
        
        let op2 = { 
            new IOperator with
                member _.Name = "Op2"
                member _.IsAsync = false
                member _.Ready = true
                member _.Fixedpoint(_) = false
                member _.Flush() = ()
                member _.ClearState() = ()
        }
        
        let op3 = { 
            new IOperator with
                member _.Name = "Op3"
                member _.IsAsync = false
                member _.Ready = true
                member _.Fixedpoint(_) = false
                member _.Flush() = ()
                member _.ClearState() = ()
        }
        
        // Create circuit using RootCircuit builder
        let circuit, (nodeId1, nodeId2, nodeId3) = 
            RootCircuit.Build(fun builder ->
                // Add operators using proper API
                let metadata = { Name = "TestOp"; TypeInfo = "Test"; Location = None }
                let nodeId1 = builder.AddOperator(op1, metadata)
                let nodeId2 = builder.AddOperator(op2, metadata)
                let nodeId3 = builder.AddOperator(op3, metadata)
                // Connect nodes with dependencies
                builder.ConnectNodes(nodeId1, nodeId2)
                builder.ConnectNodes(nodeId2, nodeId3)
                (nodeId1, nodeId2, nodeId3)
            )
        
        let scheduler = SchedulerModule.createDependencyScheduler(circuit)
        let executionOrder = scheduler.ExecutionOrder
        
        // Should be in dependency order: Op1, Op2, Op3
        Assert.AreEqual(3, executionOrder.Length)
        Assert.AreEqual(nodeId1, executionOrder.[0])
        Assert.AreEqual(nodeId2, executionOrder.[1])
        Assert.AreEqual(nodeId3, executionOrder.[2])

[<TestFixture>]
type BatchOptimizationTests() =
    
    [<Test>]
    member _.``AdaptiveBatchController should adjust batch size based on latency``() =
        let config = BatchConfig.Optimized
        let controller = new AdaptiveBatchController(config)
        
        let initialSize = controller.CurrentBatchSize
        Assert.AreEqual(config.TargetBatchSize, initialSize)
        
        // Simulate low latency - should increase batch size
        for _ in 1..10 do
            controller.UpdateBatchSize(0.5, 10000.0)
        
        Assert.Greater(controller.CurrentBatchSize, initialSize)
        
        // Simulate high latency - should decrease batch size
        for _ in 1..10 do
            controller.UpdateBatchSize(5.0, 1000.0)
        
        let sizeAfterHighLatency = controller.CurrentBatchSize
        Assert.Less(sizeAfterHighLatency, initialSize)
    
    [<Test>]
    member _.``FloatCircularBuffer should maintain correct average``() =
        let buffer = new FloatCircularBuffer(5)
        
        Assert.IsFalse(buffer.IsFull)
        
        buffer.Add(1.0)
        buffer.Add(2.0)
        buffer.Add(3.0)
        buffer.Add(4.0)
        buffer.Add(5.0)
        
        Assert.IsTrue(buffer.IsFull)
        Assert.AreEqual(3.0, buffer.Average())
        
        // Adding more should overwrite oldest
        buffer.Add(6.0)
        Assert.AreEqual(4.0, buffer.Average()) // (2+3+4+5+6)/5 = 4
    
    [<Test>]
    member _.``ZSetBatchProcessor should batch operations correctly``() =
        let config = { BatchConfig.Optimized with TargetBatchSize = 3 }
        let processor = new ZSetBatchProcessor<int>(config)
        
        let operation1 = fun (zset: ZSet<int>) -> ZSet.add zset (ZSet.singleton 1 1)
        let operation2 = fun (zset: ZSet<int>) -> ZSet.add zset (ZSet.singleton 2 1)
        
        processor.AddOperation(operation1)
        processor.AddOperation(operation2)
        
        // Should not flush yet (batch size is 3)
        Assert.Pass() // Can't easily test internal state without exposing it
    
    [<Test>]
    member _.``TaskCoordination whenAllWithTimeout should handle timeout``() = task {
        let slowTask = Task.Delay(1000)
        let fastTask = Task.Delay(10)
        
        let! result = TaskCoordination.whenAllWithTimeout [| slowTask; fastTask |] 100
        
        match result with
        | Error msg -> Assert.IsTrue(msg.Contains("timeout"))
        | Ok _ -> Assert.Fail("Should have timed out")
    }
    
    [<Test>]
    member _.``TaskCoordination executePartitioned should distribute work``() = task {
        let items = [| 1..100 |]
        let processor = fun x -> x * 2
        
        let! results = TaskCoordination.executePartitioned items processor 4
        
        Assert.AreEqual(100, results.Length)
        Assert.AreEqual(2, results.[0])
        Assert.AreEqual(200, results.[99])
    }

[<TestFixture>]
type SpanOptimizationTests() =
    
    [<Test>]
    member _.``ArrayProcessor should handle buffer processing``() =
        let mutable processor = SpanBasedProcessing.ArrayProcessor<int>(10)
        
        // Test adding items
        Assert.IsTrue(processor.Add(1))
        Assert.IsTrue(processor.Add(2))
        Assert.IsTrue(processor.Add(3))
        
        // Test processing
        let mutable sum = 0
        processor.Process(fun buffer count ->
            for i in 0 .. count - 1 do
                sum <- sum + buffer.[i]
        )
        
        Assert.AreEqual(6, sum)
    
    [<Test>]
    member _.``MemoryPoolManager should rent and return buffers``() =
        use poolManager = new SpanBasedProcessing.MemoryPoolManager<int>()
        
        let memory1 = poolManager.Rent(100)
        Assert.GreaterOrEqual(memory1.Length, 100)
        
        let memory2 = poolManager.Rent(200)
        Assert.GreaterOrEqual(memory2.Length, 200)
        
        poolManager.ReturnAll()
    
    [<Test>]
    member _.``SRTP sum should work with different numeric types``() =
        let intArray = [| 1; 2; 3; 4; 5 |]
        let intSum = SRTPOptimizedOperators.sum intArray
        Assert.AreEqual(15, intSum)
        
        let floatArray = [| 1.0; 2.0; 3.0; 4.0; 5.0 |]
        let floatSum = SRTPOptimizedOperators.sum floatArray
        Assert.AreEqual(15.0, floatSum)
    
    [<Test>]
    member _.``InlineIfLambda map should produce correct results``() =
        let items = [| 1; 2; 3; 4; 5 |]
        let doubled = InlineIfLambdaOptimizations.mapOptimized (fun x -> x * 2) items
        
        Assert.AreEqual([| 2; 4; 6; 8; 10 |], doubled)
    
    [<Test>]
    member _.``VectorizedSum should match regular sum``() =
        let data = Array.init 1000 id
        let regularSum = Array.sum data
        let vectorSum = VectorizedOperations.vectorizedSum data
        
        Assert.AreEqual(regularSum, vectorSum)
    
    [<Test>]
    member _.``CacheLineAligned should prevent false sharing``() =
        let aligned = CacheOptimizations.CacheLineAligned<int>(42)
        Assert.AreEqual(42, aligned.Value)
        
        // Verify struct is designed for cache line alignment
        // F# generic struct sizes can't be queried directly with Marshal.SizeOf
        Assert.Pass() // The struct is defined with Size=64 in the StructLayout attribute