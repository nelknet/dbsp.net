```

BenchmarkDotNet v0.15.2, macOS Sequoia 15.6.1 (24G90) [Darwin 24.6.0]
Apple M4 Max, 1 CPU, 16 logical and 16 physical cores
.NET SDK 9.0.304
  [Host] : .NET 9.0.8 (9.0.825.36511), Arm64 RyuJIT AdvSIMD DEBUG

Job=QuickInProc  Toolchain=InProcessEmitToolchain  IterationCount=5  
WarmupCount=1  

```
| Method              | DataSize | ChangeCount | Mean       | Error    | StdDev   | Gen0     | Gen1     | Allocated  |
|-------------------- |--------- |------------ |-----------:|---------:|---------:|---------:|---------:|-----------:|
| **Incremental_Circuit** | **100000**   | **1**           |   **109.4 μs** |  **1.89 μs** |  **0.49 μs** |   **1.5869** |        **-** |   **13.13 KB** |
| **Incremental_Circuit** | **100000**   | **10**          |   **132.9 μs** |  **4.13 μs** |  **1.07 μs** |  **10.2539** |   **0.2441** |   **85.52 KB** |
| **Incremental_Circuit** | **100000**   | **100**         | **1,220.7 μs** | **91.77 μs** | **23.83 μs** | **302.7344** |  **89.8438** | **2477.87 KB** |
| **Incremental_Circuit** | **1000000**  | **1**           |   **112.9 μs** |  **1.70 μs** |  **0.44 μs** |   **1.5869** |        **-** |   **13.13 KB** |
| **Incremental_Circuit** | **1000000**  | **10**          |   **148.0 μs** |  **3.57 μs** |  **0.93 μs** |  **14.4043** |   **0.9766** |  **118.31 KB** |
| **Incremental_Circuit** | **1000000**  | **100**         | **2,191.2 μs** | **33.80 μs** |  **5.23 μs** | **527.3438** | **179.6875** | **4330.17 KB** |
