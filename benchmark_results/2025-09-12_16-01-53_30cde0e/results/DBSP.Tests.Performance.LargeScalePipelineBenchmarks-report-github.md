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
| **DBSP_Incremental_ApplyOnly** | **100000**   | **1**           |  **10.79 ms** |  **0.613 ms** | **0.159 ms** |
| DBSP_Incremental_Pipeline  | 100000   | 1           |  86.22 ms | 11.039 ms | 2.867 ms |
| **DBSP_Incremental_ApplyOnly** | **100000**   | **100**         |  **11.39 ms** |  **0.117 ms** | **0.030 ms** |
| DBSP_Incremental_Pipeline  | 100000   | 100         |  80.43 ms |  1.589 ms | 0.413 ms |
| **DBSP_Incremental_ApplyOnly** | **300000**   | **1**           |  **16.18 ms** |  **0.578 ms** | **0.150 ms** |
| DBSP_Incremental_Pipeline  | 300000   | 1           | 250.07 ms |  5.010 ms | 1.301 ms |
| **DBSP_Incremental_ApplyOnly** | **300000**   | **100**         |  **36.58 ms** |  **7.241 ms** | **1.120 ms** |
| DBSP_Incremental_Pipeline  | 300000   | 100         | 263.91 ms | 10.440 ms | 1.616 ms |
