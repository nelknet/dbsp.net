```

BenchmarkDotNet v0.15.2, macOS Sequoia 15.6.1 (24G90) [Darwin 24.6.0]
Apple M4 Max, 1 CPU, 16 logical and 16 physical cores
.NET SDK 9.0.304
  [Host]   : .NET 9.0.8 (9.0.825.36511), Arm64 RyuJIT AdvSIMD DEBUG
  ShortRun : .NET 9.0.8 (9.0.825.36511), Arm64 RyuJIT AdvSIMD


```
| Method                | Job      | Toolchain              | IterationCount | LaunchCount | WarmupCount | SetSize | Mean        | Error        | StdDev    | StdErr    | Min         | Max         | Q1          | Q3          | Median      | Op/s     | Gen0       | Gen1      | Gen2     | Allocated |
|---------------------- |--------- |----------------------- |--------------- |------------ |------------ |-------- |------------:|-------------:|----------:|----------:|------------:|------------:|------------:|------------:|------------:|---------:|-----------:|----------:|---------:|----------:|
| **InnerJoin_Incremental** | **InProc**   | **InProcessEmitToolchain** | **Default**        | **Default**     | **Default**     | **100**     |    **446.9 μs** |      **3.65 μs** |   **3.41 μs** |   **0.88 μs** |    **441.4 μs** |    **454.2 μs** |    **444.0 μs** |    **448.8 μs** |    **447.1 μs** | **2,237.88** |   **160.1563** |   **50.2930** |        **-** |   **1.28 MB** |
| InnerJoin_Incremental | ShortRun | Default                | 3              | 1           | 3           | 100     |    438.7 μs |     30.37 μs |   1.66 μs |   0.96 μs |    436.8 μs |    439.9 μs |    438.1 μs |    439.6 μs |    439.4 μs | 2,279.50 |   160.1563 |   54.1992 |        - |   1.28 MB |
| **InnerJoin_Incremental** | **InProc**   | **InProcessEmitToolchain** | **Default**        | **Default**     | **Default**     | **1000**    |  **7,712.5 μs** |     **45.02 μs** |  **39.91 μs** |  **10.67 μs** |  **7,645.6 μs** |  **7,788.4 μs** |  **7,694.5 μs** |  **7,736.2 μs** |  **7,717.2 μs** |   **129.66** |  **1757.8125** |  **484.3750** | **179.6875** |  **14.06 MB** |
| InnerJoin_Incremental | ShortRun | Default                | 3              | 1           | 3           | 1000    |  7,546.1 μs |    773.97 μs |  42.42 μs |  24.49 μs |  7,497.3 μs |  7,573.8 μs |  7,532.2 μs |  7,570.5 μs |  7,567.2 μs |   132.52 |  1765.6250 |  523.4375 | 203.1250 |  14.09 MB |
| **InnerJoin_Incremental** | **InProc**   | **InProcessEmitToolchain** | **Default**        | **Default**     | **Default**     | **5000**    | **50,346.1 μs** |    **363.01 μs** | **321.80 μs** |  **86.00 μs** | **49,676.4 μs** | **51,037.9 μs** | **50,222.5 μs** | **50,540.7 μs** | **50,358.5 μs** |    **19.86** | **10000.0000** | **2562.5000** | **875.0000** |  **72.92 MB** |
| InnerJoin_Incremental | ShortRun | Default                | 3              | 1           | 3           | 5000    | 48,536.4 μs | 13,290.45 μs | 728.49 μs | 420.60 μs | 48,110.4 μs | 49,377.6 μs | 48,115.9 μs | 48,749.5 μs | 48,121.4 μs |    20.60 |  9818.1818 | 2272.7273 | 727.2727 |  72.99 MB |
