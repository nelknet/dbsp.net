```

BenchmarkDotNet v0.15.2, macOS Sequoia 15.6.1 (24G90) [Darwin 24.6.0]
Apple M4 Max, 1 CPU, 16 logical and 16 physical cores
.NET SDK 9.0.304
  [Host] : .NET 9.0.8 (9.0.825.36511), Arm64 RyuJIT AdvSIMD DEBUG

Job=QuickInProc  Toolchain=InProcessEmitToolchain  IterationCount=5  
WarmupCount=1  

```
| Method             | Size  | Delta | Mean        | Error       | StdDev    | Ratio | Gen0    | Gen1   | Allocated | Alloc Ratio |
|------------------- |------ |------ |------------:|------------:|----------:|------:|--------:|-------:|----------:|------------:|
| **ToSeq_Enumerate**    | **1000**  | **100**   |  **8,046.5 ns** |   **199.08 ns** |  **51.70 ns** |  **1.00** |  **2.8839** |      **-** |   **24208 B** |        **1.00** |
| Iter_Enumerate     | 1000  | 100   |  6,207.9 ns |    55.24 ns |  14.35 ns |  0.77 |  3.3493 |      - |   28072 B |        1.16 |
| ContainsKey_Random | 1000  | 100   |    140.1 ns |     1.82 ns |   0.47 ns |  0.02 |       - |      - |         - |        0.00 |
| TryFind_Random     | 1000  | 100   |    346.2 ns |     2.94 ns |   0.76 ns |  0.04 |  0.2866 |      - |    2400 B |        0.10 |
| UnionMany_Deltas   | 1000  | 100   | 11,716.1 ns |   109.42 ns |  28.42 ns |  1.46 | 10.8185 | 1.2665 |   90488 B |        3.74 |
|                    |       |       |             |             |           |       |         |        |           |             |
| **ToSeq_Enumerate**    | **10000** | **100**   | **77,538.4 ns** | **1,064.46 ns** | **276.44 ns** | **1.000** | **28.6865** |      **-** |  **240209 B** |       **1.000** |
| Iter_Enumerate     | 10000 | 100   | 59,055.6 ns |   688.93 ns | 178.91 ns | 0.762 | 33.4473 | 4.1504 |  280072 B |       1.166 |
| ContainsKey_Random | 10000 | 100   |    140.4 ns |     1.48 ns |   0.38 ns | 0.002 |       - |      - |         - |       0.000 |
| TryFind_Random     | 10000 | 100   |    350.6 ns |     7.03 ns |   1.82 ns | 0.005 |  0.2866 |      - |    2400 B |       0.010 |
| UnionMany_Deltas   | 10000 | 100   | 11,860.8 ns |   156.81 ns |  24.27 ns | 0.153 | 10.8185 | 1.5259 |   90488 B |       0.377 |
