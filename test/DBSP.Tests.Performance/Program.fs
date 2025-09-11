/// Entry point for DBSP performance benchmarks
module DBSP.Tests.Performance.Program

open System
open System.IO
open BenchmarkDotNet.Running
open BenchmarkDotNet.Configs
open BenchmarkDotNet.Exporters
open BenchmarkDotNet.Exporters.Json
open BenchmarkDotNet.Jobs

[<EntryPoint>]
let main args =
    // Configure BenchmarkDotNet to output JSON for BDNA analysis
    let timestamp = DateTime.UtcNow.ToString("yyyy-MM-dd_HH-mm-ss")
    let commitSha = 
        try
            let gitProcess = new System.Diagnostics.Process()
            gitProcess.StartInfo.FileName <- "git"
            gitProcess.StartInfo.Arguments <- "rev-parse --short HEAD"
            gitProcess.StartInfo.UseShellExecute <- false
            gitProcess.StartInfo.RedirectStandardOutput <- true
            gitProcess.Start() |> ignore
            let output = gitProcess.StandardOutput.ReadToEnd().Trim()
            gitProcess.WaitForExit()
            if gitProcess.ExitCode = 0 then output else "unknown"
        with
        | _ -> "unknown"
    
    let resultsDir = Path.Combine("../../benchmark_results", $"{timestamp}_{commitSha}")
    Directory.CreateDirectory(resultsDir) |> ignore
    
    let config = 
        DefaultConfig.Instance
            .AddExporter(JsonExporter.Full)
            .WithArtifactsPath(resultsDir)
            .AddJob(Job.InProcess.WithId("InProc"))
    
    let switcher = BenchmarkSwitcher [|
        // Core data structure benchmarks
        typeof<DataStructureBenchmarks.DataStructureComparisonBenchmarks>
        typeof<DataStructureBenchmarks.ZSetOperationBenchmarks>
        
        // Advanced data structure comparison benchmarks
        typeof<DataStructureComparisonBenchmarks.DataStructureCoreBenchmarks>
        typeof<DataStructureComparisonBenchmarks.ZSetOperationBenchmarks>
        typeof<DataStructureComparisonBenchmarks.MemoryAllocationBenchmarks>
        typeof<DataStructureComparisonBenchmarks.CacheLocalityBenchmarks>
        typeof<AlgebraicDispatchBenchmarks.AlgebraicDispatchBenchmarks>
        typeof<AlgebraicDispatchBenchmarks.MemoryAllocationAnalysis>
        typeof<AlgebraicDispatchBenchmarks.StructVsRefTypeBenchmarks>
        
        // Operator performance benchmarks
        typeof<OperatorBenchmarks.LinearOperatorBenchmarks>
        typeof<OperatorBenchmarks.BinaryOperatorBenchmarks>
        typeof<OperatorBenchmarks.AggregationOperatorBenchmarks>
        typeof<OperatorBenchmarks.JoinOperatorBenchmarks>
        typeof<OperatorBenchmarks.AsyncOverheadBenchmarks>
        // Join projection fusion microbenchmarks
        typeof<JoinProjectionBenchmarks>
        // Storage engine benchmarks
        typeof<DBSP.Tests.Performance.StorageBenchmarks>
        
        // Phase 6 Profiling benchmarks
        typeof<SimpleProfilingBenchmarks.CoreBottleneckBenchmarks>
        typeof<SimpleProfilingBenchmarks.AllocationAnalysis>
        
        // Large-scale scenario benchmarks
        typeof<LargeScalePipelineBenchmarks>
        // Circuit-driven large-scale incremental pipeline benchmark
        typeof<CircuitLargeScaleBenchmarks>
    |]
    
    let summary = switcher.Run(args, config)
    printfn "Benchmark results saved to: %s" resultsDir
    printfn "Commit SHA: %s" commitSha
    0
