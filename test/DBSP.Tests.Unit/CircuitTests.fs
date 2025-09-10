module DBSP.Tests.Unit.CircuitTests

open System
open System.Threading.Tasks
open NUnit.Framework
open DBSP.Circuit

[<TestFixture>]
type CircuitBuilderTests() =
    
    [<Test>]
    member _.``CircuitBuilder creates circuit with input and output`` () =
        let (circuit, inputHandle) = 
            CircuitBuilderModule.build (fun builder ->
                let input = builder.AddInput<int>("test_input")
                let output = builder.AddOutput(input, "test_output")
                input
            )
        
        Assert.That(circuit.InputHandles.Count, Is.EqualTo 1)
        Assert.That(circuit.OutputHandles.Count, Is.EqualTo 1)
        Assert.That(circuit.InputHandles.ContainsKey("test_input"), Is.True)
        Assert.That(circuit.OutputHandles.ContainsKey("test_output"), Is.True)
    
    [<Test>]
    member _.``CircuitBuilder generates unique node IDs`` () =
        let builder = CircuitBuilderModule.create()
        let metadata = { Name = "Test"; TypeInfo = "int"; Location = None }
        
        let nodeId1 = builder.AddOperator("operator1", metadata)
        let nodeId2 = builder.AddOperator("operator2", metadata)
        
        Assert.That(nodeId1, Is.Not.EqualTo nodeId2)

[<TestFixture>]  
type CircuitRuntimeTests() =
    
    [<Test>]
    member _.``CircuitRuntime starts and executes steps`` () = task {
        let (circuit, _) = 
            CircuitBuilderModule.build (fun builder ->
                builder.AddInput<int>("numbers")
            )
        
        let runtime = CircuitRuntimeModule.create circuit RuntimeConfig.Default
        
        // Start runtime
        let startResult = runtime.Start()
        Assert.That(startResult, Is.EqualTo (Ok ()))
        Assert.That(runtime.State, Is.EqualTo CircuitState.Running)
        
        // Execute step
        let! stepResult = runtime.ExecuteStepAsync()
        match stepResult with
        | Ok () -> Assert.Pass()
        | Error msg -> Assert.Fail($"Step execution failed: {msg}")
        Assert.That(runtime.StepsExecuted, Is.EqualTo 1L)
    }
    
    [<Test>]
    member _.``CircuitRuntime handles state transitions`` () =
        let (circuit, _) = CircuitBuilderModule.build (fun builder -> builder.AddInput<string>("data"))
        let runtime = CircuitRuntimeModule.create circuit RuntimeConfig.Default
        
        // Initial state
        Assert.That(runtime.State, Is.EqualTo CircuitState.Created)
        
        // Start
        let _ = runtime.Start()
        Assert.That(runtime.State, Is.EqualTo CircuitState.Running)
        
        // Pause
        let _ = runtime.Pause()
        Assert.That(runtime.State, Is.EqualTo CircuitState.Paused)

[<TestFixture>]
type HandleTests() =
    
    [<Test>]  
    member _.``InputHandle sends and receives data`` () = task {
        let inputHandle = HandleFactoryModule.createInput<int> "test"
        
        // Send data
        let! sendResult = inputHandle.SendAsync(42)
        match sendResult with
        | Ok () -> Assert.Pass()
        | Error msg -> Assert.Fail($"Send failed: {msg}")
        
        // Complete input
        inputHandle.Complete()
        
        // Should be able to read the sent data
        let (success, value) = inputHandle.Reader.TryRead()
        Assert.That(success, Is.True)
        Assert.That(value, Is.EqualTo 42)
    }
    
    [<Test>]
    member _.``OutputHandle publishes and caches values`` () = task {
        let outputHandle = HandleFactoryModule.createOutput<string> "test"
        
        // Publish value
        do! outputHandle.PublishAsync("Hello")
        
        // Should cache current value
        let currentValue = outputHandle.GetCurrentValue()
        Assert.That(currentValue, Is.EqualTo (Some "Hello"))
    }
