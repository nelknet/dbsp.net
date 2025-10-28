```

BenchmarkDotNet v0.15.2, macOS Sequoia 15.6.1 (24G90) [Darwin 24.6.0]
Apple M4 Max, 1 CPU, 16 logical and 16 physical cores
.NET SDK 9.0.304
  [Host]   : .NET 9.0.8 (9.0.825.36511), Arm64 RyuJIT AdvSIMD DEBUG
  ShortRun : .NET 9.0.8 (9.0.825.36511), Arm64 RyuJIT AdvSIMD


```
| Method                     | Job         | Toolchain              | IterationCount | LaunchCount | WarmupCount | DataSize | ChangeCount | Mean      | Error      | StdDev    |
|--------------------------- |------------ |----------------------- |--------------- |------------ |------------ |--------- |------------ |----------:|-----------:|----------:|
| **DBSP_Incremental_ApplyOnly** | **QuickInProc** | **InProcessEmitToolchain** | **5**              | **Default**     | **1**           | **100000**   | **1**           | **11.012 ms** |  **0.2608 ms** | **0.0677 ms** |
| DBSP_Incremental_ApplyOnly | ShortRun    | Default                | 3              | 1           | 3           | 100000   | 1           | 11.963 ms |  1.7053 ms | 0.0935 ms |
| **DBSP_Incremental_ApplyOnly** | **QuickInProc** | **InProcessEmitToolchain** | **5**              | **Default**     | **1**           | **100000**   | **100**         | **11.600 ms** |  **0.3172 ms** | **0.0824 ms** |
| DBSP_Incremental_ApplyOnly | ShortRun    | Default                | 3              | 1           | 3           | 100000   | 100         | 12.452 ms |  1.1652 ms | 0.0639 ms |
| **DBSP_Incremental_ApplyOnly** | **QuickInProc** | **InProcessEmitToolchain** | **5**              | **Default**     | **1**           | **300000**   | **1**           |  **7.712 ms** |  **1.7772 ms** | **0.4615 ms** |
| DBSP_Incremental_ApplyOnly | ShortRun    | Default                | 3              | 1           | 3           | 300000   | 1           |  8.279 ms |  8.3746 ms | 0.4590 ms |
| **DBSP_Incremental_ApplyOnly** | **QuickInProc** | **InProcessEmitToolchain** | **5**              | **Default**     | **1**           | **300000**   | **100**         | **21.391 ms** | **25.0842 ms** | **3.8818 ms** |
| DBSP_Incremental_ApplyOnly | ShortRun    | Default                | 3              | 1           | 3           | 300000   | 100         | 25.995 ms | 12.8839 ms | 0.7062 ms |
