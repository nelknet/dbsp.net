```

BenchmarkDotNet v0.15.2, macOS Sequoia 15.6.1 (24G90) [Darwin 24.6.0]
Apple M4 Max, 1 CPU, 16 logical and 16 physical cores
.NET SDK 9.0.304
  [Host] : .NET 9.0.8 (9.0.825.36511), Arm64 RyuJIT AdvSIMD DEBUG

Job=QuickInProc  Toolchain=InProcessEmitToolchain  IterationCount=5  
WarmupCount=1  

```
| Method                     | DataSize | ChangeCount | Mean       | Error     | StdDev    | Ratio | RatioSD |
|--------------------------- |--------- |------------ |-----------:|----------:|----------:|------:|--------:|
| **Naive_Recompute_All**        | **100000**   | **1**           |  **14.382 ms** | **1.6790 ms** | **0.4360 ms** |  **1.00** |    **0.04** |
| DBSP_Incremental_ApplyOnly | 100000   | 1           |   4.591 ms | 0.9911 ms | 0.2574 ms |  0.32 |    0.02 |
| DBSP_Incremental_Pipeline  | 100000   | 1           |  29.537 ms | 1.0273 ms | 0.2668 ms |  2.06 |    0.06 |
|                            |          |             |            |           |           |       |         |
| **Naive_Recompute_All**        | **100000**   | **100**         |  **14.452 ms** | **2.2066 ms** | **0.5730 ms** |  **1.00** |    **0.05** |
| DBSP_Incremental_ApplyOnly | 100000   | 100         |   4.247 ms | 0.7043 ms | 0.1090 ms |  0.29 |    0.01 |
| DBSP_Incremental_Pipeline  | 100000   | 100         |  29.996 ms | 2.9317 ms | 0.7614 ms |  2.08 |    0.09 |
|                            |          |             |            |           |           |       |         |
| **Naive_Recompute_All**        | **300000**   | **1**           |  **48.971 ms** | **4.1035 ms** | **1.0657 ms** |  **1.00** |    **0.03** |
| DBSP_Incremental_ApplyOnly | 300000   | 1           |  23.075 ms | 2.9090 ms | 0.7555 ms |  0.47 |    0.02 |
| DBSP_Incremental_Pipeline  | 300000   | 1           | 119.239 ms | 7.8724 ms | 2.0444 ms |  2.44 |    0.06 |
|                            |          |             |            |           |           |       |         |
| **Naive_Recompute_All**        | **300000**   | **100**         |  **47.606 ms** | **0.6980 ms** | **0.1080 ms** |  **1.00** |    **0.00** |
| DBSP_Incremental_ApplyOnly | 300000   | 100         |  21.457 ms | 1.5286 ms | 0.2366 ms |  0.45 |    0.00 |
| DBSP_Incremental_Pipeline  | 300000   | 100         | 112.666 ms | 9.4553 ms | 2.4555 ms |  2.37 |    0.05 |
