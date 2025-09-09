```

BenchmarkDotNet v0.15.2, macOS Sequoia 15.6.1 (24G90) [Darwin 24.6.0]
Apple M4 Max, 1 CPU, 16 logical and 16 physical cores
.NET SDK 10.0.100-preview.7.25380.108
  [Host]     : .NET 9.0.8 (9.0.825.36511), Arm64 RyuJIT AdvSIMD DEBUG
  DefaultJob : .NET 9.0.8 (9.0.825.36511), Arm64 RyuJIT AdvSIMD


```
| Method                 | OperationCount | Mean          | Error        | StdDev       | Ratio | Completed Work Items | Lock Contentions | Gen0        | Gen1        | Allocated     | Alloc Ratio |
|----------------------- |--------------- |--------------:|-------------:|-------------:|------:|---------------------:|-----------------:|------------:|------------:|--------------:|------------:|
| **ZSet_RepeatedAdditions** | **1000**           |   **7,098.78 μs** |    **41.022 μs** |    **36.365 μs** | **1.000** |                    **-** |                **-** |   **5304.6875** |    **992.1875** |   **43352.55 KB** |       **1.000** |
| ZSet_BatchConstruction | 1000           |      38.85 μs |     0.115 μs |     0.102 μs | 0.005 |                    - |                - |     20.9961 |      4.5776 |     171.83 KB |       0.004 |
|                        |                |               |              |              |       |                      |                  |             |             |               |             |
| **ZSet_RepeatedAdditions** | **10000**          | **884,894.43 μs** | **7,780.130 μs** | **6,074.214 μs** | **1.000** |                    **-** |                **-** | **526000.0000** | **219000.0000** | **4301395.94 KB** |       **1.000** |
| ZSet_BatchConstruction | 10000          |     562.91 μs |    11.017 μs |    10.821 μs | 0.001 |                    - |                - |    209.9609 |     93.7500 |     1718.7 KB |       0.000 |
