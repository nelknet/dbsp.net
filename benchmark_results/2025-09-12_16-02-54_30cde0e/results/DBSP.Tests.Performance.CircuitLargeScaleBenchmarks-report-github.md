```

BenchmarkDotNet v0.15.2, macOS Sequoia 15.6.1 (24G90) [Darwin 24.6.0]
Apple M4 Max, 1 CPU, 16 logical and 16 physical cores
.NET SDK 9.0.304
  [Host] : .NET 9.0.8 (9.0.825.36511), Arm64 RyuJIT AdvSIMD DEBUG

Job=QuickInProc  Toolchain=InProcessEmitToolchain  IterationCount=5  
WarmupCount=1  

```
| Method              | DataSize | ChangeCount | Mean     | Error    | StdDev  | Gen0    | Gen1    | Allocated |
|-------------------- |--------- |------------ |---------:|---------:|--------:|--------:|--------:|----------:|
| **Incremental_Circuit** | **100000**   | **1**           | **105.3 μs** |  **1.58 μs** | **0.41 μs** |  **1.0986** |       **-** |   **9.35 KB** |
| **Incremental_Circuit** | **100000**   | **10**          | **114.8 μs** |  **0.55 μs** | **0.14 μs** |  **4.6387** |  **0.1221** |  **38.46 KB** |
| **Incremental_Circuit** | **100000**   | **100**         | **264.8 μs** | **14.63 μs** | **2.26 μs** | **57.1289** |       **-** | **468.35 KB** |
| **Incremental_Circuit** | **1000000**  | **1**           | **103.4 μs** |  **0.47 μs** | **0.12 μs** |  **1.0986** |       **-** |   **9.35 KB** |
| **Incremental_Circuit** | **1000000**  | **10**          | **118.8 μs** |  **5.13 μs** | **1.33 μs** |  **5.8594** |  **0.1221** |  **48.12 KB** |
| **Incremental_Circuit** | **1000000**  | **100**         | **322.3 μs** | **18.75 μs** | **4.87 μs** | **77.6367** | **21.4844** | **637.88 KB** |
