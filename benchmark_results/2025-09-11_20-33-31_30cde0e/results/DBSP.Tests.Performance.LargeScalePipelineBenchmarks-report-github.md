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
| **Naive_Recompute_All**        | **100000**   | **1**           |  **17.176 ms** |  **1.6621 ms** | **0.4316 ms** |  **1.00** |    **0.03** |
| DBSP_Incremental_ApplyOnly | 100000   | 1           |   3.890 ms |  0.2737 ms | 0.0711 ms |  0.23 |    0.01 |
| DBSP_Incremental_Pipeline  | 100000   | 1           |  55.257 ms |  3.7075 ms | 0.9628 ms |  3.22 |    0.09 |
|                            |          |             |            |            |           |       |         |
| **Naive_Recompute_All**        | **100000**   | **100**         |  **16.862 ms** |  **0.9950 ms** | **0.2584 ms** |  **1.00** |    **0.02** |
| DBSP_Incremental_ApplyOnly | 100000   | 100         |   3.981 ms |  0.2868 ms | 0.0745 ms |  0.24 |    0.01 |
| DBSP_Incremental_Pipeline  | 100000   | 100         |  54.912 ms |  1.5194 ms | 0.2351 ms |  3.26 |    0.05 |
|                            |          |             |            |            |           |       |         |
| **Naive_Recompute_All**        | **300000**   | **1**           |  **59.073 ms** | **18.2654 ms** | **4.7435 ms** |  **1.01** |    **0.11** |
| DBSP_Incremental_ApplyOnly | 300000   | 1           |  19.401 ms |  1.0487 ms | 0.2723 ms |  0.33 |    0.03 |
| DBSP_Incremental_Pipeline  | 300000   | 1           | 204.153 ms |  8.7733 ms | 2.2784 ms |  3.48 |    0.29 |
|                            |          |             |            |            |           |       |         |
| **Naive_Recompute_All**        | **300000**   | **100**         |  **59.226 ms** | **18.0180 ms** | **4.6792 ms** |  **1.01** |    **0.11** |
| DBSP_Incremental_ApplyOnly | 300000   | 100         |  19.762 ms |  0.8839 ms | 0.1368 ms |  0.34 |    0.03 |
| DBSP_Incremental_Pipeline  | 300000   | 100         | 200.813 ms |  3.4875 ms | 0.9057 ms |  3.41 |    0.28 |
