```

BenchmarkDotNet v0.15.2, macOS Sequoia 15.6.1 (24G90) [Darwin 24.6.0]
Apple M4 Max, 1 CPU, 16 logical and 16 physical cores
.NET SDK 9.0.304
  [Host] : .NET 9.0.8 (9.0.825.36511), Arm64 RyuJIT AdvSIMD DEBUG

Job=QuickInProc  Toolchain=InProcessEmitToolchain  IterationCount=5  
WarmupCount=1  

```
| Method              | DataSize | ChangeCount | Mean        | Error     | StdDev    | Gen0     | Gen1     | Allocated  |
|-------------------- |--------- |------------ |------------:|----------:|----------:|---------:|---------:|-----------:|
| **Incremental_Circuit** | **100000**   | **1**           |    **98.53 μs** |  **2.435 μs** |  **0.632 μs** |   **1.7090** |        **-** |   **14.27 KB** |
| **Incremental_Circuit** | **100000**   | **10**          |   **127.98 μs** |  **7.607 μs** |  **1.975 μs** |  **11.9629** |   **0.2441** |   **97.76 KB** |
| **Incremental_Circuit** | **100000**   | **100**         | **1,167.41 μs** | **44.042 μs** | **11.438 μs** | **300.7813** |  **95.7031** |  **2462.1 KB** |
| **Incremental_Circuit** | **1000000**  | **1**           |   **102.72 μs** |  **3.245 μs** |  **0.843 μs** |   **1.7090** |        **-** |   **14.27 KB** |
| **Incremental_Circuit** | **1000000**  | **10**          |   **139.26 μs** |  **3.127 μs** |  **0.484 μs** |  **16.1133** |   **0.9766** |  **132.35 KB** |
| **Incremental_Circuit** | **1000000**  | **100**         | **2,071.50 μs** | **46.767 μs** |  **7.237 μs** | **511.7188** | **179.6875** | **4208.68 KB** |
