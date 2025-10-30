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
| **DBSP_Incremental_ApplyOnly** | **100000**   | **1**           |  **28.88 ms** |  **0.698 ms** | **0.181 ms** |
| DBSP_Incremental_Pipeline  | 100000   | 1           |  96.45 ms |  2.344 ms | 0.609 ms |
| **DBSP_Incremental_ApplyOnly** | **100000**   | **100**         |  **30.83 ms** |  **1.869 ms** | **0.485 ms** |
| DBSP_Incremental_Pipeline  | 100000   | 100         |  96.14 ms |  2.427 ms | 0.376 ms |
| **DBSP_Incremental_ApplyOnly** | **300000**   | **1**           | **105.09 ms** | **13.571 ms** | **2.100 ms** |
| DBSP_Incremental_Pipeline  | 300000   | 1           | 330.66 ms | 12.259 ms | 1.897 ms |
| **DBSP_Incremental_ApplyOnly** | **300000**   | **100**         | **106.60 ms** |  **9.353 ms** | **1.447 ms** |
| DBSP_Incremental_Pipeline  | 300000   | 100         | 340.34 ms | 10.800 ms | 2.805 ms |
