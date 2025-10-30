```

BenchmarkDotNet v0.15.2, macOS Sequoia 15.6.1 (24G90) [Darwin 24.6.0]
Apple M4 Max, 1 CPU, 16 logical and 16 physical cores
.NET SDK 9.0.304
  [Host] : .NET 9.0.8 (9.0.825.36511), Arm64 RyuJIT AdvSIMD DEBUG

Job=QuickInProc  Toolchain=InProcessEmitToolchain  IterationCount=5  
WarmupCount=1  

```
| Method              | DataSize | ChangeCount | Mean     | Error    | StdDev   | Gen0    | Gen1    | Allocated |
|-------------------- |--------- |------------ |---------:|---------:|---------:|--------:|--------:|----------:|
| **Incremental_Circuit** | **100000**   | **1**           | **102.3 μs** |  **1.25 μs** |  **0.32 μs** |  **1.0986** |       **-** |    **9.3 KB** |
| **Incremental_Circuit** | **100000**   | **10**          | **112.8 μs** |  **6.36 μs** |  **0.98 μs** |  **4.6387** |  **0.1221** |   **38.6 KB** |
| **Incremental_Circuit** | **100000**   | **100**         | **249.2 μs** | **10.40 μs** |  **2.70 μs** | **57.3730** |       **-** | **470.51 KB** |
| **Incremental_Circuit** | **1000000**  | **1**           | **144.0 μs** | **73.32 μs** | **11.35 μs** |  **1.0986** |       **-** |   **9.31 KB** |
| **Incremental_Circuit** | **1000000**  | **10**          | **116.7 μs** |  **3.54 μs** |  **0.55 μs** |  **5.8594** |       **-** |  **48.31 KB** |
| **Incremental_Circuit** | **1000000**  | **100**         | **300.9 μs** | **17.46 μs** |  **4.53 μs** | **78.1250** | **19.5313** | **640.53 KB** |
