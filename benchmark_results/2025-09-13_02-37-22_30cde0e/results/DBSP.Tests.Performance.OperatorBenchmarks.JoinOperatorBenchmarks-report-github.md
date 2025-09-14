```

BenchmarkDotNet v0.15.2, macOS Sequoia 15.6.1 (24G90) [Darwin 24.6.0]
Apple M4 Max, 1 CPU, 16 logical and 16 physical cores
.NET SDK 9.0.304
  [Host]   : .NET 9.0.8 (9.0.825.36511), Arm64 RyuJIT AdvSIMD DEBUG
  ShortRun : .NET 9.0.8 (9.0.825.36511), Arm64 RyuJIT AdvSIMD


```
| Method                | Job      | Toolchain              | IterationCount | LaunchCount | WarmupCount | SetSize | Mean        | Error        | StdDev      | StdErr    | Min         | Max         | Q1          | Q3          | Median      | Op/s     | Gen0      | Gen1      | Gen2     | Allocated |
|---------------------- |--------- |----------------------- |--------------- |------------ |------------ |-------- |------------:|-------------:|------------:|----------:|------------:|------------:|------------:|------------:|------------:|---------:|----------:|----------:|---------:|----------:|
| **InnerJoin_Incremental** | **InProc**   | **InProcessEmitToolchain** | **Default**        | **Default**     | **Default**     | **100**     |    **610.9 μs** |     **19.71 μs** |    **55.92 μs** |   **5.80 μs** |    **473.9 μs** |    **759.0 μs** |    **578.4 μs** |    **636.5 μs** |    **605.3 μs** | **1,636.89** |  **151.3672** |   **57.6172** |        **-** |   **1.21 MB** |
| InnerJoin_Incremental | ShortRun | Default                | 3              | 1           | 3           | 100     |    625.9 μs |    641.25 μs |    35.15 μs |  20.29 μs |    592.6 μs |    662.7 μs |    607.5 μs |    642.6 μs |    622.4 μs | 1,597.67 |  151.3672 |   59.5703 |        - |   1.21 MB |
| **InnerJoin_Incremental** | **InProc**   | **InProcessEmitToolchain** | **Default**        | **Default**     | **Default**     | **1000**    | **10,107.6 μs** |    **249.46 μs** |   **731.63 μs** |  **73.53 μs** |  **8,737.5 μs** | **11,886.3 μs** |  **9,365.2 μs** | **10,399.8 μs** | **10,194.1 μs** |    **98.94** | **1578.1250** |  **578.1250** | **140.6250** |  **12.69 MB** |
| InnerJoin_Incremental | ShortRun | Default                | 3              | 1           | 3           | 1000    | 10,662.6 μs | 10,771.64 μs |   590.43 μs | 340.88 μs | 10,168.6 μs | 11,316.5 μs | 10,335.6 μs | 10,909.6 μs | 10,502.6 μs |    93.79 | 1593.7500 |  593.7500 | 187.5000 |  12.73 MB |
| **InnerJoin_Incremental** | **InProc**   | **InProcessEmitToolchain** | **Default**        | **Default**     | **Default**     | **5000**    | **73,430.7 μs** |  **1,762.28 μs** | **5,112.69 μs** | **519.12 μs** | **59,586.8 μs** | **86,618.0 μs** | **69,949.1 μs** | **76,382.3 μs** | **74,623.5 μs** |    **13.62** | **9062.5000** | **2875.0000** | **937.5000** |  **64.74 MB** |
| InnerJoin_Incremental | ShortRun | Default                | 3              | 1           | 3           | 5000    | 64,214.0 μs | 12,364.15 μs |   677.72 μs | 391.28 μs | 63,435.5 μs | 64,672.5 μs | 63,984.8 μs | 64,603.2 μs | 64,534.0 μs |    15.57 | 8857.1429 | 3000.0000 | 857.1429 |  64.55 MB |
