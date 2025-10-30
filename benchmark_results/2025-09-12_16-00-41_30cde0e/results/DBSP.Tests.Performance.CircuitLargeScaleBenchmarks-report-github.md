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
| **Incremental_Circuit** | **100000**   | **1**           |   **100.5 μs** |  **1.64 μs** |  **0.43 μs** |   **1.4648** |        **-** |   **12.62 KB** |
| **Incremental_Circuit** | **100000**   | **10**          |   **125.7 μs** |  **3.79 μs** |  **0.98 μs** |   **9.7656** |   **0.2441** |   **81.63 KB** |
| **Incremental_Circuit** | **100000**   | **100**         | **1,239.2 μs** | **59.18 μs** | **15.37 μs** | **296.8750** |   **1.9531** | **2432.55 KB** |
| **Incremental_Circuit** | **1000000**  | **1**           |   **105.1 μs** |  **2.53 μs** |  **0.39 μs** |   **1.4648** |        **-** |   **12.62 KB** |
| **Incremental_Circuit** | **1000000**  | **10**          |   **141.6 μs** |  **6.35 μs** |  **1.65 μs** |  **13.6719** |   **0.7324** |  **112.95 KB** |
| **Incremental_Circuit** | **1000000**  | **100**         | **2,251.8 μs** | **47.55 μs** | **12.35 μs** | **519.5313** | **183.5938** | **4270.55 KB** |
