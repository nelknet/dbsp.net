```

BenchmarkDotNet v0.15.2, macOS Sequoia 15.6.1 (24G90) [Darwin 24.6.0]
Apple M4 Max, 1 CPU, 16 logical and 16 physical cores
.NET SDK 9.0.304
  [Host] : .NET 9.0.8 (9.0.825.36511), Arm64 RyuJIT AdvSIMD DEBUG

Job=QuickInProc  Toolchain=InProcessEmitToolchain  IterationCount=5  
WarmupCount=1  

```
| Method                     | DataSize | ChangeCount | Mean       | Error      | StdDev    | Ratio | RatioSD |
|--------------------------- |--------- |------------ |-----------:|-----------:|----------:|------:|--------:|
| **Naive_Recompute_All**        | **100000**   | **1**           |  **14.238 ms** |  **0.4819 ms** | **0.1251 ms** |  **1.00** |    **0.01** |
| DBSP_Incremental_ApplyOnly | 100000   | 1           |   4.414 ms |  1.2509 ms | 0.3249 ms |  0.31 |    0.02 |
| DBSP_Incremental_Pipeline  | 100000   | 1           |  26.364 ms |  2.8364 ms | 0.7366 ms |  1.85 |    0.05 |
|                            |          |             |            |            |           |       |         |
| **Naive_Recompute_All**        | **100000**   | **100**         |  **14.573 ms** |  **3.6991 ms** | **0.9606 ms** |  **1.00** |    **0.08** |
| DBSP_Incremental_ApplyOnly | 100000   | 100         |   4.030 ms |  0.4594 ms | 0.0711 ms |  0.28 |    0.02 |
| DBSP_Incremental_Pipeline  | 100000   | 100         |  25.350 ms |  2.0103 ms | 0.5221 ms |  1.75 |    0.11 |
|                            |          |             |            |            |           |       |         |
| **Naive_Recompute_All**        | **300000**   | **1**           |  **48.172 ms** |  **3.5029 ms** | **0.5421 ms** |  **1.00** |    **0.01** |
| DBSP_Incremental_ApplyOnly | 300000   | 1           |  21.806 ms |  4.6077 ms | 1.1966 ms |  0.45 |    0.02 |
| DBSP_Incremental_Pipeline  | 300000   | 1           | 102.904 ms | 10.7514 ms | 2.7921 ms |  2.14 |    0.06 |
|                            |          |             |            |            |           |       |         |
| **Naive_Recompute_All**        | **300000**   | **100**         |  **46.917 ms** |  **1.5880 ms** | **0.2457 ms** |  **1.00** |    **0.01** |
| DBSP_Incremental_ApplyOnly | 300000   | 100         |  19.446 ms |  4.2571 ms | 1.1056 ms |  0.41 |    0.02 |
| DBSP_Incremental_Pipeline  | 300000   | 100         |  98.570 ms |  6.5295 ms | 1.6957 ms |  2.10 |    0.03 |
