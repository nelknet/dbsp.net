```

BenchmarkDotNet v0.15.2, macOS Sequoia 15.6.1 (24G90) [Darwin 24.6.0]
Apple M4 Max, 1 CPU, 16 logical and 16 physical cores
.NET SDK 9.0.304
  [Host]   : .NET 9.0.8 (9.0.825.36511), Arm64 RyuJIT AdvSIMD DEBUG
  ShortRun : .NET 9.0.8 (9.0.825.36511), Arm64 RyuJIT AdvSIMD


```
| Method                | Job      | Toolchain              | IterationCount | LaunchCount | WarmupCount | SetSize | Mean        | Error       | StdDev      | StdErr    | Min         | Max         | Q1          | Q3          | Median      | Op/s     | Gen0      | Gen1      | Gen2     | Allocated   |
|---------------------- |--------- |----------------------- |--------------- |------------ |------------ |-------- |------------:|------------:|------------:|----------:|------------:|------------:|------------:|------------:|------------:|---------:|----------:|----------:|---------:|------------:|
| **InnerJoin_Incremental** | **InProc**   | **InProcessEmitToolchain** | **Default**        | **Default**     | **Default**     | **100**     |    **272.2 μs** |     **7.17 μs** |    **21.03 μs** |   **2.11 μs** |    **229.9 μs** |    **324.3 μs** |    **255.5 μs** |    **283.4 μs** |    **270.5 μs** | **3,673.90** |   **92.2852** |   **28.3203** |        **-** |   **756.52 KB** |
| InnerJoin_Incremental | ShortRun | Default                | 3              | 1           | 3           | 100     |    261.9 μs |   138.17 μs |     7.57 μs |   4.37 μs |    257.2 μs |    270.6 μs |    257.5 μs |    264.3 μs |    257.9 μs | 3,818.27 |   92.2852 |   28.3203 |        - |   756.52 KB |
| **InnerJoin_Incremental** | **InProc**   | **InProcessEmitToolchain** | **Default**        | **Default**     | **Default**     | **1000**    |  **4,625.6 μs** |   **108.74 μs** |   **315.47 μs** |  **32.03 μs** |  **4,008.3 μs** |  **5,230.6 μs** |  **4,352.2 μs** |  **4,796.9 μs** |  **4,663.9 μs** |   **216.19** |  **921.8750** |  **445.3125** |        **-** |  **7554.21 KB** |
| InnerJoin_Incremental | ShortRun | Default                | 3              | 1           | 3           | 1000    |  4,233.5 μs | 3,429.90 μs |   188.00 μs | 108.54 μs |  4,043.1 μs |  4,419.1 μs |  4,140.8 μs |  4,328.7 μs |  4,238.4 μs |   236.21 |  921.8750 |  445.3125 |        - |  7554.19 KB |
| **InnerJoin_Incremental** | **InProc**   | **InProcessEmitToolchain** | **Default**        | **Default**     | **Default**     | **5000**    | **42,632.2 μs** | **1,428.63 μs** | **3,982.46 μs** | **419.79 μs** | **35,582.9 μs** | **53,691.4 μs** | **40,260.5 μs** | **43,923.8 μs** | **42,771.3 μs** |    **23.46** | **5312.5000** | **2312.5000** | **687.5000** | **37758.84 KB** |
| InnerJoin_Incremental | ShortRun | Default                | 3              | 1           | 3           | 5000    | 40,624.8 μs | 9,116.24 μs |   499.69 μs | 288.50 μs | 40,112.8 μs | 41,111.2 μs | 40,381.6 μs | 40,880.8 μs | 40,650.4 μs |    24.62 | 5312.5000 | 2312.5000 | 687.5000 | 37755.74 KB |
