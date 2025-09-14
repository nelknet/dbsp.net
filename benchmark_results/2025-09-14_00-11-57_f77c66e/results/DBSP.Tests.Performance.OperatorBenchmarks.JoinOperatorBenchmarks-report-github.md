```

BenchmarkDotNet v0.15.2, macOS Sequoia 15.6.1 (24G90) [Darwin 24.6.0]
Apple M4 Max, 1 CPU, 16 logical and 16 physical cores
.NET SDK 9.0.304
  [Host]   : .NET 9.0.8 (9.0.825.36511), Arm64 RyuJIT AdvSIMD DEBUG
  ShortRun : .NET 9.0.8 (9.0.825.36511), Arm64 RyuJIT AdvSIMD


```
| Method                | Job      | Toolchain              | IterationCount | LaunchCount | WarmupCount | SetSize | Mean        | Error        | StdDev    | StdErr    | Min         | Max         | Q1          | Q3          | Median      | Op/s     | Gen0       | Gen1      | Gen2     | Allocated |
|---------------------- |--------- |----------------------- |--------------- |------------ |------------ |-------- |------------:|-------------:|----------:|----------:|------------:|------------:|------------:|------------:|------------:|---------:|-----------:|----------:|---------:|----------:|
| **InnerJoin_Incremental** | **InProc**   | **InProcessEmitToolchain** | **Default**        | **Default**     | **Default**     | **100**     |    **441.9 μs** |      **3.35 μs** |   **3.14 μs** |   **0.81 μs** |    **437.4 μs** |    **447.4 μs** |    **439.7 μs** |    **443.8 μs** |    **442.0 μs** | **2,263.21** |   **162.1094** |   **55.1758** |        **-** |    **1.3 MB** |
| InnerJoin_Incremental | ShortRun | Default                | 3              | 1           | 3           | 100     |    424.8 μs |     25.87 μs |   1.42 μs |   0.82 μs |    423.2 μs |    426.0 μs |    424.2 μs |    425.6 μs |    425.1 μs | 2,354.16 |   162.1094 |    7.3242 |        - |    1.3 MB |
| **InnerJoin_Incremental** | **InProc**   | **InProcessEmitToolchain** | **Default**        | **Default**     | **Default**     | **1000**    |  **7,622.1 μs** |     **46.86 μs** |  **43.84 μs** |  **11.32 μs** |  **7,559.4 μs** |  **7,706.3 μs** |  **7,588.0 μs** |  **7,656.1 μs** |  **7,625.1 μs** |   **131.20** |  **1789.0625** |  **687.5000** | **187.5000** |   **14.3 MB** |
| InnerJoin_Incremental | ShortRun | Default                | 3              | 1           | 3           | 1000    |  7,459.2 μs |  1,060.73 μs |  58.14 μs |  33.57 μs |  7,408.2 μs |  7,522.5 μs |  7,427.6 μs |  7,484.7 μs |  7,446.9 μs |   134.06 |  1789.0625 |  687.5000 | 203.1250 |   14.3 MB |
| **InnerJoin_Incremental** | **InProc**   | **InProcessEmitToolchain** | **Default**        | **Default**     | **Default**     | **5000**    | **52,886.9 μs** |    **707.08 μs** | **661.40 μs** | **170.77 μs** | **51,993.7 μs** | **54,334.5 μs** | **52,426.4 μs** | **53,295.4 μs** | **52,721.2 μs** |    **18.91** | **10062.5000** | **2687.5000** | **875.0000** |  **73.58 MB** |
| InnerJoin_Incremental | ShortRun | Default                | 3              | 1           | 3           | 5000    | 51,235.2 μs | 14,939.17 μs | 818.87 μs | 472.77 μs | 50,714.2 μs | 52,179.0 μs | 50,763.2 μs | 51,495.6 μs | 50,812.2 μs |    19.52 | 10100.0000 | 3000.0000 | 900.0000 |  74.03 MB |
