/// Entry point for DBSP performance benchmarks
module DBSP.Tests.Performance.Program

open BenchmarkDotNet.Running

[<EntryPoint>]
let main args =
    let switcher = BenchmarkSwitcher [|
        // Core data structure benchmarks
        typeof<DataStructureBenchmarks.DataStructureComparisonBenchmarks>
        typeof<DataStructureBenchmarks.ZSetOperationBenchmarks>
        
        // Operator performance benchmarks
        typeof<OperatorBenchmarks.LinearOperatorBenchmarks>
        typeof<OperatorBenchmarks.BinaryOperatorBenchmarks>
        typeof<OperatorBenchmarks.AggregationOperatorBenchmarks>
        typeof<OperatorBenchmarks.JoinOperatorBenchmarks>
        typeof<OperatorBenchmarks.AsyncOverheadBenchmarks>
    |]
    switcher.Run(args) |> ignore
    0