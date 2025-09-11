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
| **ToSeq_Enumerate**    | **1000**  | **100**   |  **8,008.5 ns** |   **297.82 ns** |  **77.34 ns** |  **1.00** |  **2.8839** |      **-** |   **24208 B** |        **1.00** |
| Iter_Enumerate     | 1000  | 100   |  6,407.7 ns |   331.63 ns |  51.32 ns |  0.80 |  3.3493 |      - |   28072 B |        1.16 |
| ContainsKey_Random | 1000  | 100   |    142.7 ns |     3.27 ns |   0.51 ns |  0.02 |       - |      - |         - |        0.00 |
| TryFind_Random     | 1000  | 100   |    348.2 ns |     6.18 ns |   1.60 ns |  0.04 |  0.2866 |      - |    2400 B |        0.10 |
| UnionMany_Deltas   | 1000  | 100   | 11,904.0 ns |   183.11 ns |  47.55 ns |  1.49 | 10.8185 | 1.3580 |   90488 B |        3.74 |
|                    |       |       |             |             |           |       |         |        |           |             |
| **ToSeq_Enumerate**    | **10000** | **100**   | **78,326.1 ns** | **1,266.31 ns** | **328.86 ns** | **1.000** | **28.6865** |      **-** |  **240209 B** |       **1.000** |
| Iter_Enumerate     | 10000 | 100   | 61,330.0 ns | 1,212.08 ns | 314.77 ns | 0.783 | 33.4473 | 4.1504 |  280073 B |       1.166 |
| ContainsKey_Random | 10000 | 100   |    141.3 ns |     3.55 ns |   0.55 ns | 0.002 |       - |      - |         - |       0.000 |
| TryFind_Random     | 10000 | 100   |    346.4 ns |     3.39 ns |   0.88 ns | 0.004 |  0.2866 |      - |    2400 B |       0.010 |
| UnionMany_Deltas   | 10000 | 100   | 11,899.1 ns |   133.42 ns |  34.65 ns | 0.152 | 10.8185 | 1.5259 |   90488 B |       0.377 |
