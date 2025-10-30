```

BenchmarkDotNet v0.15.2, macOS Sequoia 15.6.1 (24G90) [Darwin 24.6.0]
Apple M4 Max, 1 CPU, 16 logical and 16 physical cores
.NET SDK 9.0.304
  [Host] : .NET 9.0.8 (9.0.825.36511), Arm64 RyuJIT AdvSIMD DEBUG

Job=QuickInProc  Toolchain=InProcessEmitToolchain  IterationCount=5  
WarmupCount=1  

```
| Method                     | DataSize | ChangeCount | Mean      | Error     | StdDev   |
|--------------------------- |--------- |------------ |----------:|----------:|---------:|
| **DBSP_Incremental_ApplyOnly** | **100000**   | **1**           |  **16.11 ms** |  **0.450 ms** | **0.117 ms** |
| DBSP_Incremental_Pipeline  | 100000   | 1           | 102.47 ms |  2.322 ms | 0.359 ms |
| **DBSP_Incremental_ApplyOnly** | **100000**   | **100**         |  **17.11 ms** |  **0.392 ms** | **0.102 ms** |
| DBSP_Incremental_Pipeline  | 100000   | 100         | 101.06 ms |  2.268 ms | 0.589 ms |
| **DBSP_Incremental_ApplyOnly** | **300000**   | **1**           |  **18.57 ms** |  **0.645 ms** | **0.168 ms** |
| DBSP_Incremental_Pipeline  | 300000   | 1           | 278.13 ms |  9.983 ms | 1.545 ms |
| **DBSP_Incremental_ApplyOnly** | **300000**   | **100**         |  **43.85 ms** |  **0.997 ms** | **0.259 ms** |
| DBSP_Incremental_Pipeline  | 300000   | 100         | 317.02 ms | 29.312 ms | 4.536 ms |
