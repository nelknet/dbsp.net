```

BenchmarkDotNet v0.15.2, macOS Sequoia 15.6.1 (24G90) [Darwin 24.6.0]
Apple M4 Max, 1 CPU, 16 logical and 16 physical cores
.NET SDK 10.0.100-preview.7.25380.108
  [Host] : .NET 9.0.8 (9.0.825.36511), Arm64 RyuJIT AdvSIMD DEBUG
  Dry    : .NET 9.0.8 (9.0.825.36511), Arm64 RyuJIT AdvSIMD

Job=Dry  IterationCount=1  LaunchCount=1  
RunStrategy=ColdStart  UnrollFactor=1  WarmupCount=1  

```
| Method                 | OperationCount | Mean         | Error | Ratio | Gen0        | Completed Work Items | Lock Contentions | Gen1        | Allocated     | Alloc Ratio |
|----------------------- |--------------- |-------------:|------:|------:|------------:|---------------------:|-----------------:|------------:|--------------:|------------:|
| **ZSet_RepeatedAdditions** | **1000**           |    **36.410 ms** |    **NA** |  **1.00** |   **5000.0000** |                    **-** |                **-** |   **1000.0000** |   **43352.55 KB** |       **1.000** |
| ZSet_BatchConstruction | 1000           |     2.785 ms |    NA |  0.08 |           - |                    - |                - |           - |     171.86 KB |       0.004 |
|                        |                |              |       |       |             |                      |                  |             |               |             |
| **ZSet_RepeatedAdditions** | **10000**          | **1,006.517 ms** |    **NA** | **1.000** | **526000.0000** |                    **-** |                **-** | **219000.0000** | **4301395.94 KB** |       **1.000** |
| ZSet_BatchConstruction | 10000          |     4.841 ms |    NA | 0.005 |           - |                    - |                - |           - |    1718.73 KB |       0.000 |
