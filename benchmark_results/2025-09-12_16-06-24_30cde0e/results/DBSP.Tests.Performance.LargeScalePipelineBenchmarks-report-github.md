```

BenchmarkDotNet v0.15.2, macOS Sequoia 15.6.1 (24G90) [Darwin 24.6.0]
Apple M4 Max, 1 CPU, 16 logical and 16 physical cores
.NET SDK 9.0.304
  [Host] : .NET 9.0.8 (9.0.825.36511), Arm64 RyuJIT AdvSIMD DEBUG

Job=QuickInProc  Toolchain=InProcessEmitToolchain  IterationCount=5  
WarmupCount=1  

```
| Method                     | DataSize | ChangeCount | Mean      | Error    | StdDev   |
|--------------------------- |--------- |------------ |----------:|---------:|---------:|
| **DBSP_Incremental_ApplyOnly** | **100000**   | **1**           |  **11.59 ms** | **0.266 ms** | **0.069 ms** |
| DBSP_Incremental_Pipeline  | 100000   | 1           |  89.91 ms | 1.960 ms | 0.509 ms |
| **DBSP_Incremental_ApplyOnly** | **100000**   | **100**         |  **11.57 ms** | **0.528 ms** | **0.137 ms** |
| DBSP_Incremental_Pipeline  | 100000   | 100         |  82.97 ms | 1.022 ms | 0.158 ms |
| **DBSP_Incremental_ApplyOnly** | **300000**   | **1**           |  **15.42 ms** | **2.675 ms** | **0.695 ms** |
| DBSP_Incremental_Pipeline  | 300000   | 1           | 250.51 ms | 4.382 ms | 0.678 ms |
| **DBSP_Incremental_ApplyOnly** | **300000**   | **100**         |  **36.54 ms** | **7.957 ms** | **1.231 ms** |
| DBSP_Incremental_Pipeline  | 300000   | 100         | 271.55 ms | 6.078 ms | 1.578 ms |
