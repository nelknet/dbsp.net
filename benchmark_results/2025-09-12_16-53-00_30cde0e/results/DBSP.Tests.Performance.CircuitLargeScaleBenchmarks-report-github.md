```

BenchmarkDotNet v0.15.2, macOS Sequoia 15.6.1 (24G90) [Darwin 24.6.0]
Apple M4 Max, 1 CPU, 16 logical and 16 physical cores
.NET SDK 9.0.304
  [Host] : .NET 9.0.8 (9.0.825.36511), Arm64 RyuJIT AdvSIMD DEBUG

Job=QuickInProc  Toolchain=InProcessEmitToolchain  IterationCount=5  
WarmupCount=1  

```
| Method              | DataSize | ChangeCount | Mean         | Error       | StdDev     | Gen0     | Gen1     | Allocated  |
|-------------------- |--------- |------------ |-------------:|------------:|-----------:|---------:|---------:|-----------:|
| **Incremental_Circuit** | **100000**   | **1**           |     **2.166 μs** |   **0.0548 μs** |  **0.0142 μs** |   **2.1896** |   **0.0114** |    **17.9 KB** |
| **Incremental_Circuit** | **100000**   | **10**          |    **26.482 μs** |   **0.2872 μs** |  **0.0444 μs** |  **11.2610** |   **0.5493** |   **92.15 KB** |
| **Incremental_Circuit** | **100000**   | **100**         | **1,137.993 μs** |  **12.4040 μs** |  **3.2213 μs** | **306.6406** |  **82.0313** | **2505.85 KB** |
| **Incremental_Circuit** | **1000000**  | **1**           |     **2.188 μs** |   **0.0324 μs** |  **0.0084 μs** |   **2.1896** |   **0.0114** |    **17.9 KB** |
| **Incremental_Circuit** | **1000000**  | **10**          |    **38.075 μs** |   **0.7993 μs** |  **0.2076 μs** |  **15.3809** |   **1.0986** |  **125.75 KB** |
| **Incremental_Circuit** | **1000000**  | **100**         | **2,123.252 μs** | **119.6324 μs** | **18.5133 μs** | **531.2500** | **187.5000** | **4365.14 KB** |
