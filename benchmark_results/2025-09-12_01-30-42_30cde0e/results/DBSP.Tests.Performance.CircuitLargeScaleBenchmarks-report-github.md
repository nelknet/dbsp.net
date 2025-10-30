```

BenchmarkDotNet v0.15.2, macOS Sequoia 15.6.1 (24G90) [Darwin 24.6.0]
Apple M4 Max, 1 CPU, 16 logical and 16 physical cores
.NET SDK 9.0.304
  [Host] : .NET 9.0.8 (9.0.825.36511), Arm64 RyuJIT AdvSIMD DEBUG

Job=QuickInProc  Toolchain=InProcessEmitToolchain  IterationCount=5  
WarmupCount=1  

```
| Method              | DataSize | ChangeCount | Mean       | Error     | StdDev   | Gen0     | Gen1     | Allocated  |
|-------------------- |--------- |------------ |-----------:|----------:|---------:|---------:|---------:|-----------:|
| **Incremental_Circuit** | **100000**   | **1**           |   **101.1 μs** |   **1.54 μs** |  **0.24 μs** |   **1.0986** |        **-** |    **9.62 KB** |
| **Incremental_Circuit** | **100000**   | **10**          |   **120.2 μs** |   **3.46 μs** |  **0.90 μs** |   **7.6904** |   **0.1221** |   **63.36 KB** |
| **Incremental_Circuit** | **100000**   | **100**         | **1,083.9 μs** |  **17.15 μs** |  **2.65 μs** | **246.0938** |  **54.6875** | **2019.05 KB** |
| **Incremental_Circuit** | **1000000**  | **1**           |   **105.7 μs** |   **3.48 μs** |  **0.90 μs** |   **1.0986** |        **-** |    **9.62 KB** |
| **Incremental_Circuit** | **1000000**  | **10**          |   **140.9 μs** |   **3.03 μs** |  **0.79 μs** |  **12.6953** |   **0.7324** |  **104.77 KB** |
| **Incremental_Circuit** | **1000000**  | **100**         | **2,136.9 μs** | **183.20 μs** | **47.58 μs** | **468.7500** | **171.8750** | **3838.79 KB** |
