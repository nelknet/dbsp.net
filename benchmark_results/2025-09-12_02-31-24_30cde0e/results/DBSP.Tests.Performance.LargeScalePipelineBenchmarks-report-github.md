```

BenchmarkDotNet v0.15.2, macOS Sequoia 15.6.1 (24G90) [Darwin 24.6.0]
Apple M4 Max, 1 CPU, 16 logical and 16 physical cores
.NET SDK 9.0.304
  [Host] : .NET 9.0.8 (9.0.825.36511), Arm64 RyuJIT AdvSIMD DEBUG

Job=QuickInProc  Toolchain=InProcessEmitToolchain  IterationCount=5  
WarmupCount=1  

```
| Method                     | DataSize | ChangeCount | Mean       | Error      | StdDev    |
|--------------------------- |--------- |------------ |-----------:|-----------:|----------:|
| **DBSP_Incremental_ApplyOnly** | **100000**   | **1**           |   **3.332 ms** |  **0.2549 ms** | **0.0662 ms** |
| DBSP_Incremental_Pipeline  | 100000   | 1           |  47.423 ms |  2.9903 ms | 0.7766 ms |
| **DBSP_Incremental_ApplyOnly** | **100000**   | **100**         |   **3.447 ms** |  **0.2105 ms** | **0.0326 ms** |
| DBSP_Incremental_Pipeline  | 100000   | 100         |  52.939 ms | 43.2232 ms | 6.6888 ms |
| **DBSP_Incremental_ApplyOnly** | **300000**   | **1**           |  **13.598 ms** |  **0.3921 ms** | **0.1018 ms** |
| DBSP_Incremental_Pipeline  | 300000   | 1           | 179.975 ms |  5.5523 ms | 0.8592 ms |
| **DBSP_Incremental_ApplyOnly** | **300000**   | **100**         |  **13.643 ms** |  **0.0808 ms** | **0.0125 ms** |
| DBSP_Incremental_Pipeline  | 300000   | 100         | 181.147 ms |  9.0069 ms | 1.3938 ms |
