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
| **Naive_Recompute_All**        | **100000**   | **1**           |  **15.527 ms** |  **3.0613 ms** | **0.7950 ms** |  **1.00** |    **0.07** |
| DBSP_Incremental_ApplyOnly | 100000   | 1           |   4.155 ms |  0.9460 ms | 0.2457 ms |  0.27 |    0.02 |
| DBSP_Incremental_Pipeline  | 100000   | 1           |  25.298 ms |  1.2821 ms | 0.3329 ms |  1.63 |    0.08 |
|                            |          |             |            |            |           |       |         |
| **Naive_Recompute_All**        | **100000**   | **100**         |  **15.257 ms** |  **3.2355 ms** | **0.8403 ms** |  **1.00** |    **0.07** |
| DBSP_Incremental_ApplyOnly | 100000   | 100         |   4.308 ms |  0.7149 ms | 0.1857 ms |  0.28 |    0.02 |
| DBSP_Incremental_Pipeline  | 100000   | 100         |  25.623 ms |  1.9396 ms | 0.5037 ms |  1.68 |    0.09 |
|                            |          |             |            |            |           |       |         |
| **Naive_Recompute_All**        | **300000**   | **1**           |  **48.241 ms** |  **1.3890 ms** | **0.3607 ms** |  **1.00** |    **0.01** |
| DBSP_Incremental_ApplyOnly | 300000   | 1           |  25.356 ms |  0.8138 ms | 0.2113 ms |  0.53 |    0.01 |
| DBSP_Incremental_Pipeline  | 300000   | 1           | 114.894 ms | 32.3800 ms | 8.4090 ms |  2.38 |    0.16 |
|                            |          |             |            |            |           |       |         |
| **Naive_Recompute_All**        | **300000**   | **100**         |  **51.188 ms** |  **5.6335 ms** | **1.4630 ms** |  **1.00** |    **0.04** |
| DBSP_Incremental_ApplyOnly | 300000   | 100         |  22.392 ms |  2.9657 ms | 0.7702 ms |  0.44 |    0.02 |
| DBSP_Incremental_Pipeline  | 300000   | 100         | 103.459 ms |  4.9243 ms | 1.2788 ms |  2.02 |    0.06 |
