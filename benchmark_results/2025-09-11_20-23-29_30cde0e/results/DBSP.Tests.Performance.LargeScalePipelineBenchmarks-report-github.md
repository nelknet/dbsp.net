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
| **Naive_Recompute_All**        | **100000**   | **1**           |  **15.492 ms** |  **0.5413 ms** | **0.1406 ms** |  **1.00** |    **0.01** |
| DBSP_Incremental_ApplyOnly | 100000   | 1           |  15.010 ms |  0.2815 ms | 0.0731 ms |  0.97 |    0.01 |
| DBSP_Incremental_Pipeline  | 100000   | 1           |  66.837 ms |  1.8490 ms | 0.4802 ms |  4.31 |    0.05 |
|                            |          |             |            |            |           |       |         |
| **Naive_Recompute_All**        | **100000**   | **100**         |  **15.042 ms** |  **3.2431 ms** | **0.8422 ms** |  **1.00** |    **0.07** |
| DBSP_Incremental_ApplyOnly | 100000   | 100         |  16.924 ms |  0.4406 ms | 0.1144 ms |  1.13 |    0.06 |
| DBSP_Incremental_Pipeline  | 100000   | 100         |  68.290 ms |  2.8331 ms | 0.4384 ms |  4.55 |    0.24 |
|                            |          |             |            |            |           |       |         |
| **Naive_Recompute_All**        | **300000**   | **1**           |  **48.335 ms** | **12.3512 ms** | **1.9114 ms** |  **1.00** |    **0.05** |
| DBSP_Incremental_ApplyOnly | 300000   | 1           |   7.372 ms |  0.3312 ms | 0.0860 ms |  0.15 |    0.01 |
| DBSP_Incremental_Pipeline  | 300000   | 1           | 177.349 ms | 12.3131 ms | 3.1977 ms |  3.67 |    0.15 |
|                            |          |             |            |            |           |       |         |
| **Naive_Recompute_All**        | **300000**   | **100**         |  **48.600 ms** |  **5.6964 ms** | **1.4793 ms** |  **1.00** |    **0.04** |
| DBSP_Incremental_ApplyOnly | 300000   | 100         |  56.379 ms |  1.6255 ms | 0.4221 ms |  1.16 |    0.03 |
| DBSP_Incremental_Pipeline  | 300000   | 100         | 224.804 ms |  6.1332 ms | 0.9491 ms |  4.63 |    0.14 |
