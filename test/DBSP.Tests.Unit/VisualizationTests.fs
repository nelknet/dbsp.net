module DBSP.Tests.Unit.VisualizationTests

open System
open NUnit.Framework
open DBSP.Circuit
open System.IO

[<TestFixture>]
type CircuitVisualizationTests() =
    
    [<Test>]
    member _.``CircuitVisualizer generates DOT format`` () =
        let (circuit, _) = 
            CircuitBuilderModule.build (fun builder ->
                let input = builder.AddInput<int>("numbers")
                let metadata = { Name = "Double"; TypeInfo = "int -> int"; Location = None }
                let nodeId = builder.AddOperator("doubler", metadata)
                builder.ConnectNodes(input.NodeId, nodeId)
                let output = builder.AddOutput(input, "doubled")
                (input, output)
            )
        
        let dotGraph = CircuitVisualization.generateDot circuit (Some "Test Circuit")
        
        // Verify DOT format structure
        Assert.IsTrue(dotGraph.Contains("digraph"))
        Assert.IsTrue(dotGraph.Contains("numbers"))
        Assert.IsTrue(dotGraph.Contains("doubled"))
        Assert.IsTrue(dotGraph.Contains("->"))
    
    [<Test>]
    member _.``CircuitVisualizer generates text representation`` () =
        let (circuit, _) =
            CircuitBuilderModule.build (fun builder ->
                let input1 = builder.AddInput<int>("data1")
                let input2 = builder.AddInput<string>("data2")  
                let output1 = builder.AddOutput(input1, "result1")
                let output2 = builder.AddOutput(input2, "result2")
                (input1, input2, output1, output2)
            )
        
        let textRepr = CircuitVisualization.generateTextRepresentation circuit
        
        // Verify text contains expected sections
        Assert.IsTrue(textRepr.Contains("Circuit Topology:"))
        Assert.IsTrue(textRepr.Contains("data1"))
        Assert.IsTrue(textRepr.Contains("data2"))
    
    [<Test>]
    member _.``CircuitVisualizer saves DOT file correctly`` () =
        let (circuit, _) =
            CircuitBuilderModule.build (fun builder ->
                let input = builder.AddInput<double>("sensor_data")
                builder.AddOutput(input, "processed_data")
            )
        
        let tempFile = Path.GetTempFileName() + ".dot"
        
        try
            // Save DOT file
            CircuitVisualization.saveDotFile circuit tempFile (Some "Sensor Processing")
            
            // Verify file exists and has content
            Assert.IsTrue(File.Exists(tempFile))
            let content = File.ReadAllText(tempFile)
            Assert.Greater(content.Length, 50)
            Assert.IsTrue(content.Contains("digraph"))
            
        finally
            // Cleanup
            if File.Exists(tempFile) then
                File.Delete(tempFile)
    
    [<Test>]
    member _.``CircuitVisualizer handles complex circuit topology`` () =
        let (circuit, _) =
            CircuitBuilderModule.build (fun builder ->
                let input = builder.AddInput<int>("source")
                let metadata = { Name = "Transform"; TypeInfo = "int -> int"; Location = None }
                
                // Create branching topology
                let node1 = builder.AddOperator("branch1", metadata)
                let node2 = builder.AddOperator("branch2", metadata)
                let node3 = builder.AddOperator("merge", metadata)
                
                builder.ConnectNodes(input.NodeId, node1)
                builder.ConnectNodes(input.NodeId, node2)
                builder.ConnectNodes(node1, node3)
                builder.ConnectNodes(node2, node3)
                
                let output = builder.AddOutput(input, "result")
                output
            )
        
        let statistics = (CircuitVisualization.createVisualizer circuit).GetCircuitStatistics()
        
        // Verify statistics reflect complex topology
        Assert.Greater(statistics.OperatorCount, 2)
        Assert.Greater(statistics.ConnectionCount, 2)
        Assert.Greater(statistics.ComplexityScore, 4)
        
        let dotGraph = CircuitVisualization.generateDot circuit None
        
        // Verify DOT contains branching structure - check length as proxy
        Assert.Greater(dotGraph.Length, 200) // Complex circuit should have substantial DOT output
    
    [<Test>]
    member _.``CircuitVisualizer exports analysis data`` () =
        let (circuit, _) =
            CircuitBuilderModule.build (fun builder ->
                let input = builder.AddInput<string>("text")
                let metadata = { Name = "Process"; TypeInfo = "string -> string"; Location = None }
                let processor = builder.AddOperator("text_processor", metadata)
                builder.ConnectNodes(input.NodeId, processor)
                builder.AddOutput(input, "processed_text")
            )
        
        let analysisData = CircuitVisualization.exportForAnalysis circuit
        
        // Verify export structure
        Assert.IsNotNull(analysisData.Nodes)
        Assert.IsNotNull(analysisData.Edges)  
        Assert.IsNotNull(analysisData.Statistics)
        Assert.Greater(analysisData.Nodes.Length, 0)
        Assert.Greater(analysisData.Statistics.ComplexityScore, 0)