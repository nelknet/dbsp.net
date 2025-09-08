namespace DBSP.Circuit

open System
open System.IO
open System.Runtime.CompilerServices
open System.Text

/// Visual edge representation for circuit graphs
type VisualEdge = {
    Source: string
    Target: string
    Label: string option
} with
    /// Generate DOT edge representation
    member this.ToDot() =
        let label = 
            match this.Label with
            | Some l -> $" [label=\"{l}\"]"
            | None -> ""
        $"\"{this.Source}\" -> \"{this.Target}\"{label};\n"

/// Visual node representation for circuit graphs  
type VisualNode = {
    Id: string
    Label: string
    Shape: string
    Color: string option
} with
    /// Generate DOT node representation
    member this.ToDot() =
        let colorAttr = 
            match this.Color with
            | Some c -> $", fillcolor=\"{c}\", style=\"filled\""
            | None -> ""
        $"\"{this.Id}\" [label=\"{this.Label}\", shape={this.Shape}{colorAttr}];\n"

/// Circuit visualization generator for debugging and monitoring
type CircuitVisualizer internal (circuit: CircuitDefinition) =
    
    /// Convert node ID to visual identifier
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let nodeIdToVisual (nodeId: NodeId) = $"node_{nodeId.Id}"
    
    /// Generate visual nodes from circuit operators
    let generateVisualNodes() =
        let nodes = ResizeArray<VisualNode>()
        
        // Add input nodes
        for kvp in circuit.InputHandles do
            let inputName = kvp.Key
            // Extract NodeId without casting the generic type
            let nodeId = 
                let handleType = kvp.Value.GetType()
                let nodeIdProp = handleType.GetProperty("NodeId")
                nodeIdProp.GetValue(kvp.Value) :?> NodeId
            
            nodes.Add({
                Id = nodeIdToVisual nodeId
                Label = $"Input: {inputName}"
                Shape = "ellipse"
                Color = Some "lightblue"
            })
        
        // Add operator nodes
        for kvp in circuit.Operators do
            let nodeId = kvp.Key
            let operator = kvp.Value
            let operatorType = operator.GetType().Name
            nodes.Add({
                Id = nodeIdToVisual nodeId
                Label = $"{operatorType}\\nNode: {nodeId.Id}"
                Shape = "box"
                Color = Some "lightgray"
            })
        
        // Add output nodes
        for kvp in circuit.OutputHandles do
            let outputName = kvp.Key
            // Extract NodeId using reflection to avoid generic cast issues
            let nodeId = 
                let handleType = kvp.Value.GetType()
                let nodeIdProp = handleType.GetProperty("NodeId")
                nodeIdProp.GetValue(kvp.Value) :?> NodeId
            
            nodes.Add({
                Id = nodeIdToVisual nodeId
                Label = $"Output: {outputName}" 
                Shape = "ellipse"
                Color = Some "lightgreen"
            })
        
        nodes |> List.ofSeq
    
    /// Generate visual edges from circuit connections
    let generateVisualEdges() =
        circuit.Connections
        |> List.map (fun (source, target) ->
            {
                Source = nodeIdToVisual source
                Target = nodeIdToVisual target
                Label = None
            })
    
    /// Generate complete DOT graph representation
    member _.GenerateDot(?title: string) =
        let graphTitle = defaultArg title "DBSP Circuit"
        let nodes = generateVisualNodes()
        let edges = generateVisualEdges()
        
        let sb = StringBuilder()
        sb.AppendLine($"digraph \"{graphTitle}\" {{") |> ignore
        sb.AppendLine("  rankdir=LR;") |> ignore
        sb.AppendLine("  node [fontname=\"Helvetica\"];") |> ignore
        sb.AppendLine("  edge [fontname=\"Helvetica\"];") |> ignore
        sb.AppendLine() |> ignore
        
        // Add nodes
        sb.AppendLine("  // Nodes") |> ignore
        for node in nodes do
            sb.Append("  ").Append(node.ToDot()) |> ignore
        
        sb.AppendLine() |> ignore
        
        // Add edges  
        sb.AppendLine("  // Edges") |> ignore
        for edge in edges do
            sb.Append("  ").Append(edge.ToDot()) |> ignore
        
        sb.AppendLine("}") |> ignore
        sb.ToString()
    
    /// Save circuit graph to DOT file
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    member this.SaveDotFile(filepath: string, ?title: string) =
        let dotContent = this.GenerateDot(?title = title)
        File.WriteAllText(filepath, dotContent)
    
    /// Get circuit statistics for visualization metadata
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    member _.GetCircuitStatistics() = 
        let operatorCount = circuit.Operators.Count
        let connectionCount = circuit.Connections.Length
        let inputCount = circuit.InputHandles.Count
        let outputCount = circuit.OutputHandles.Count
        
        {|
            OperatorCount = operatorCount
            ConnectionCount = connectionCount
            InputCount = inputCount
            OutputCount = outputCount
            ComplexityScore = operatorCount + connectionCount
        |}

/// Circuit visualization and debugging utilities
module CircuitVisualization =
    
    /// Create visualizer for circuit
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let createVisualizer (circuit: CircuitDefinition) = 
        CircuitVisualizer(circuit)
    
    /// Generate DOT representation for circuit
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let generateDot (circuit: CircuitDefinition) (title: string option) = 
        let visualizer = createVisualizer circuit
        match title with
        | Some t -> visualizer.GenerateDot(t)
        | None -> visualizer.GenerateDot()
    
    /// Save circuit as DOT file for GraphViz rendering
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let saveDotFile (circuit: CircuitDefinition) (filepath: string) (title: string option) =
        let visualizer = createVisualizer circuit
        match title with
        | Some t -> visualizer.SaveDotFile(filepath, t)
        | None -> visualizer.SaveDotFile(filepath)
    
    /// Generate simple text representation of circuit topology
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let generateTextRepresentation (circuit: CircuitDefinition) =
        let sb = StringBuilder()
        sb.AppendLine("Circuit Topology:") |> ignore
        sb.AppendLine("================") |> ignore
        
        // Input handles
        sb.AppendLine("Inputs:") |> ignore
        for kvp in circuit.InputHandles do
            sb.AppendLine($"  {kvp.Key}") |> ignore
        
        // Operators
        sb.AppendLine("Operators:") |> ignore  
        for kvp in circuit.Operators do
            let nodeId = kvp.Key
            let op = kvp.Value
            sb.AppendLine($"  {nodeId.Id}: {op.GetType().Name}") |> ignore
        
        // Connections
        sb.AppendLine("Connections:") |> ignore
        for (source, target) in circuit.Connections do
            sb.AppendLine($"  {source.Id} -> {target.Id}") |> ignore
        
        // Outputs
        sb.AppendLine("Outputs:") |> ignore
        for kvp in circuit.OutputHandles do
            sb.AppendLine($"  {kvp.Key}") |> ignore
        
        sb.ToString()
    
    /// Quick visualization helper for debugging
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let debugPrint (circuit: CircuitDefinition) = 
        let text = generateTextRepresentation circuit
        printfn "%s" text
        
    /// Export circuit for external graph analysis tools
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let exportForAnalysis (circuit: CircuitDefinition) =
        {|
            Nodes = circuit.Operators.Keys |> Seq.map (fun n -> n.Id) |> List.ofSeq
            Edges = circuit.Connections |> List.map (fun (s, t) -> (s.Id, t.Id))
            Statistics = (createVisualizer circuit).GetCircuitStatistics()
        |}