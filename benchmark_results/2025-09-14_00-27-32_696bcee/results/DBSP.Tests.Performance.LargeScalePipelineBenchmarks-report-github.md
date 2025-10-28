```

BenchmarkDotNet v0.15.2, macOS Sequoia 15.6.1 (24G90) [Darwin 24.6.0]
Apple M4 Max, 1 CPU, 16 logical and 16 physical cores
.NET SDK 9.0.304
  [Host]   : .NET 9.0.8 (9.0.825.36511), Arm64 RyuJIT AdvSIMD DEBUG
  ShortRun : .NET 9.0.8 (9.0.825.36511), Arm64 RyuJIT AdvSIMD


```
| Method                    | Job         | Toolchain              | IterationCount | LaunchCount | WarmupCount | DataSize | ChangeCount | Mean      | Error      | StdDev   |
|-------------------------- |------------ |----------------------- |--------------- |------------ |------------ |--------- |------------ |----------:|-----------:|---------:|
| **DBSP_Incremental_Pipeline** | **QuickInProc** | **InProcessEmitToolchain** | **5**              | **Default**     | **1**           | **100000**   | **1**           |  **88.29 ms** |   **6.498 ms** | **1.687 ms** |
| DBSP_Incremental_Pipeline | ShortRun    | Default                | 3              | 1           | 3           | 100000   | 1           |  86.29 ms |   5.208 ms | 0.285 ms |
| **DBSP_Incremental_Pipeline** | **QuickInProc** | **InProcessEmitToolchain** | **5**              | **Default**     | **1**           | **100000**   | **100**         |  **83.23 ms** |   **3.951 ms** | **1.026 ms** |
| DBSP_Incremental_Pipeline | ShortRun    | Default                | 3              | 1           | 3           | 100000   | 100         |  86.50 ms |  14.616 ms | 0.801 ms |
| **DBSP_Incremental_Pipeline** | **QuickInProc** | **InProcessEmitToolchain** | **5**              | **Default**     | **1**           | **300000**   | **1**           | **256.12 ms** |  **18.679 ms** | **2.891 ms** |
| DBSP_Incremental_Pipeline | ShortRun    | Default                | 3              | 1           | 3           | 300000   | 1           | 268.13 ms | 127.462 ms | 6.987 ms |
| **DBSP_Incremental_Pipeline** | **QuickInProc** | **InProcessEmitToolchain** | **5**              | **Default**     | **1**           | **300000**   | **100**         | **287.54 ms** |  **36.240 ms** | **9.412 ms** |
| DBSP_Incremental_Pipeline | ShortRun    | Default                | 3              | 1           | 3           | 300000   | 100         | 312.47 ms | 116.660 ms | 6.395 ms |
