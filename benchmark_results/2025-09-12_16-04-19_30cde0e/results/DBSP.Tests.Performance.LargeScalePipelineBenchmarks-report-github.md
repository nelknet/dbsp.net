```

BenchmarkDotNet v0.15.2, macOS Sequoia 15.6.1 (24G90) [Darwin 24.6.0]
Apple M4 Max, 1 CPU, 16 logical and 16 physical cores
.NET SDK 9.0.304
  [Host] : .NET 9.0.8 (9.0.825.36511), Arm64 RyuJIT AdvSIMD DEBUG

Job=QuickInProc  Toolchain=InProcessEmitToolchain  IterationCount=5  
WarmupCount=1  

```
| Method                     | DataSize | ChangeCount | Mean       | Error      | StdDev    |
|--------------------------- |--------- |------------ |-----------:|-----------:|----------:|
| **DBSP_Incremental_ApplyOnly** | **100000**   | **1**           |   **3.509 ms** |  **0.0951 ms** | **0.0147 ms** |
| DBSP_Incremental_Pipeline  | 100000   | 1           |  50.707 ms |  6.2467 ms | 1.6222 ms |
| **DBSP_Incremental_ApplyOnly** | **100000**   | **100**         |   **3.724 ms** |  **0.3508 ms** | **0.0911 ms** |
| DBSP_Incremental_Pipeline  | 100000   | 100         |  48.517 ms |  2.6383 ms | 0.4083 ms |
| **DBSP_Incremental_ApplyOnly** | **300000**   | **1**           |  **14.963 ms** |  **1.4833 ms** | **0.3852 ms** |
| DBSP_Incremental_Pipeline  | 300000   | 1           | 188.199 ms | 14.5142 ms | 3.7693 ms |
| **DBSP_Incremental_ApplyOnly** | **300000**   | **100**         |  **14.658 ms** |  **0.6523 ms** | **0.1694 ms** |
| DBSP_Incremental_Pipeline  | 300000   | 100         | 188.702 ms | 22.1686 ms | 5.7571 ms |
