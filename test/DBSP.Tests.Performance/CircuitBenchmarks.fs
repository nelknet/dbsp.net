namespace DBSP.Tests.Performance

open System
open System.Threading.Tasks
open BenchmarkDotNet.Attributes
open BenchmarkDotNet.Configs
open BenchmarkDotNet.Jobs
open DBSP.Circuit

/// Performance benchmarks for DBSP circuit construction and execution
[<MemoryDiagnoser>]
[<MinColumn>] [<MaxColumn>] [<Q1Column>] [<Q3Column>] [<AllStatisticsColumn>]
[<SimpleJob(RuntimeMoniker.Net80)>]
type CircuitConstructionBenchmarks() =
    
    [<Params(10, 100, 1000)>]
    member val OperatorCount = 0 with get, set
    
    [<Benchmark>]
    member this.CircuitBuilder_SimpleChain() =
        let (circuit, _) = 
            CircuitBuilderModule.build (fun builder ->
                let mutable current = builder.AddInput<int>("input")
                let metadata = { Name = "Transform"; TypeInfo = "int -> int"; Location = None }
                
                // Create chain of operators
                for i = 1 to this.OperatorCount do
                    let nodeId = builder.AddOperator($"op_{i}", metadata)
                    builder.ConnectNodes(current.NodeId, nodeId)
                    current <- { current with NodeId = nodeId }
                
                let output = builder.AddOutput(current, "output")
                output
            )
        
        circuit.Operators.Count
    
    [<Benchmark>]
    member this.CircuitBuilder_ParallelBranches() =
        let (circuit, _) = 
            CircuitBuilderModule.build (fun builder ->
                let input = builder.AddInput<int>("input")
                let metadata = { Name = "Branch"; TypeInfo = "int -> string"; Location = None }
                let outputs = ResizeArray<StreamHandle<int>>()
                
                // Create parallel branches
                for i = 1 to this.OperatorCount do
                    let nodeId = builder.AddOperator($"branch_{i}", metadata)
                    builder.ConnectNodes(input.NodeId, nodeId)
                    let output = builder.AddOutput(input, $"output_{i}")
                    outputs.Add(output)
                
                outputs |> List.ofSeq
            )
        
        circuit.Operators.Count

/// Performance benchmarks for circuit execution and task coordination
[<MemoryDiagnoser>]
[<MinColumn>] [<MaxColumn>] [<Q1Column>] [<Q3Column>] [<AllStatisticsColumn>]
[<SimpleJob(RuntimeMoniker.Net80)>]
type CircuitExecutionBenchmarks() =
    let mutable circuit: CircuitDefinition = Unchecked.defaultof<_>
    let mutable runtime: CircuitRuntime = Unchecked.defaultof<_>
    
    [<Params(1, 2, 4, 8)>]
    member val WorkerThreads = 0 with get, set
    
    [<Params(10, 100, 1000)>]
    member val StepCount = 0 with get, set
    
    [<GlobalSetup>]
    member this.Setup() =
        let (builtCircuit, _) = 
            CircuitBuilderModule.build (fun builder ->
                let input = builder.AddInput<int>("data")
                let output = builder.AddOutput(input, "results")
                (input, output)
            )
        
        circuit <- builtCircuit
        
        let config = { RuntimeConfig.Default with WorkerThreads = this.WorkerThreads }
        runtime <- CircuitRuntimeModule.create circuit config
        
        // Start runtime
        runtime.Start() |> ignore
    
    [<GlobalCleanup>]
    member this.Cleanup() =
        runtime.Terminate() |> ignore
    
    [<Benchmark(Baseline = true)>]
    member this.Sequential_Step_Execution() = async {
        let mutable totalSteps = 0L
        
        for _ = 1 to this.StepCount do
            let! result = runtime.ExecuteStepAsync() |> Async.AwaitTask
            match result with
            | Ok () -> totalSteps <- totalSteps + 1L
            | Error _ -> ()
        
        return totalSteps
    }
    
    [<Benchmark>]
    member this.Circuit_Throughput_Steps_Per_Second() = async {
        let startTime = DateTime.UtcNow
        let mutable completedSteps = 0L
        
        for _ = 1 to this.StepCount do
            let! result = runtime.ExecuteStepAsync() |> Async.AwaitTask
            match result with
            | Ok () -> completedSteps <- completedSteps + 1L
            | Error _ -> ()
        
        let totalTime = DateTime.UtcNow - startTime
        let stepsPerSecond = double completedSteps / totalTime.TotalSeconds
        
        return stepsPerSecond
    }

/// Benchmarks for async I/O handle performance with .NET Channels
[<MemoryDiagnoser>]
[<MinColumn>] [<MaxColumn>] [<Q1Column>] [<Q3Column>] [<AllStatisticsColumn>] 
[<SimpleJob(RuntimeMoniker.Net80)>]
type HandlePerformanceBenchmarks() =
    
    [<Params(1000, 10000, 100000)>]
    member val MessageCount = 0 with get, set
    
    [<Benchmark(Baseline = true)>]
    member this.InputHandle_Sequential_Send() = async {
        let inputHandle = HandleFactoryModule.createInput<int> "perf_test"
        let mutable successCount = 0
        
        for i = 1 to this.MessageCount do
            let! result = inputHandle.SendAsync(i) |> Async.AwaitTask
            match result with
            | Ok () -> successCount <- successCount + 1
            | Error _ -> ()
        
        inputHandle.Complete()
        return successCount
    }
    
    [<Benchmark>]
    member this.OutputHandle_Sequential_Publish() = async {
        let outputHandle = HandleFactoryModule.createOutput<string> "perf_test"
        
        for i = 1 to this.MessageCount do
            do! outputHandle.PublishAsync($"Message_{i}") |> Async.AwaitTask
        
        return this.MessageCount
    }
    
    [<Benchmark>]
    member this.InputHandle_Channel_Throughput() = async {
        let inputHandle = HandleFactoryModule.createInput<int> "throughput_test"
        let startTime = DateTime.UtcNow
        
        // Background task to consume data
        let consumerTask = Task.Run(fun () -> async {
            let mutable count = 0
            try
                while count < this.MessageCount do
                    let (success, _) = inputHandle.Reader.TryRead()
                    if success then
                        count <- count + 1
                    else
                        do! Async.Sleep(1)
                return count
            with
            | _ -> return count
        })
        
        // Send data
        for i = 1 to this.MessageCount do
            let! _ = inputHandle.SendAsync(i) |> Async.AwaitTask
            ()
        
        inputHandle.Complete()
        let! consumedCount = consumerTask |> Async.AwaitTask
        
        let elapsed = DateTime.UtcNow - startTime
        let throughput = double this.MessageCount / elapsed.TotalSeconds
        
        return throughput
    }

/// Comprehensive circuit latency and scheduling benchmarks
[<MemoryDiagnoser>]
[<MinColumn>] [<MaxColumn>] [<Q1Column>] [<Q3Column>] [<AllStatisticsColumn>]
[<SimpleJob(RuntimeMoniker.Net80)>]
type CircuitSchedulingBenchmarks() =
    
    [<Params(5, 10, 20, 50)>]
    member val CircuitComplexity = 0 with get, set
    
    [<Benchmark>]
    member this.TopologicalSort_Dependency_Resolution() =
        let (circuit, _) = 
            CircuitBuilderModule.build (fun builder ->
                let input = builder.AddInput<int>("source")
                let mutable nodes = [input.NodeId]
                let metadata = { Name = "Node"; TypeInfo = "int -> int"; Location = None }
                
                // Create complex dependency graph
                for level = 1 to this.CircuitComplexity do
                    let newNodes = ResizeArray<NodeId>()
                    for prevNode in nodes do
                        for branch = 1 to 2 do // Binary branching
                            let nodeId = builder.AddOperator($"L{level}_B{branch}", metadata)
                            builder.ConnectNodes(prevNode, nodeId)
                            newNodes.Add(nodeId)
                    nodes <- newNodes |> List.ofSeq
                
                nodes
            )
        
        let scheduler = SchedulerModule.createDependencyScheduler circuit
        let executionOrder = scheduler.ExecutionOrder
        
        // Return scheduling efficiency metric
        executionOrder.Length
    
    [<Benchmark>]
    member this.Circuit_Memory_Allocation_Pattern() =
        let mutable totalAllocations = 0L
        
        for iteration = 1 to 10 do
            let beforeGC = GC.GetTotalMemory(true)
            
            let (circuit, _) = 
                CircuitBuilderModule.build (fun builder ->
                    let input = builder.AddInput<int[]>("data_arrays")
                    
                    // Memory-intensive operations
                    for i = 1 to this.CircuitComplexity do
                        let metadata = { 
                            Name = $"MemoryOp_{i}"
                            TypeInfo = "int[] -> int[]"
                            Location = None 
                        }
                        let nodeId = builder.AddOperator($"array_processor_{i}", metadata)
                        builder.ConnectNodes(input.NodeId, nodeId)
                    
                    builder.AddOutput(input, "processed_arrays")
                )
            
            let afterGC = GC.GetTotalMemory(false)
            let allocatedBytes = afterGC - beforeGC
            totalAllocations <- totalAllocations + allocatedBytes
        
        totalAllocations / 10L // Average allocation per circuit construction