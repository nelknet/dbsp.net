```

BenchmarkDotNet v0.15.2, macOS Sequoia 15.6.1 (24G90) [Darwin 24.6.0]
Apple M4 Max, 1 CPU, 16 logical and 16 physical cores
.NET SDK 9.0.304
  [Host]   : .NET 9.0.8 (9.0.825.36511), Arm64 RyuJIT AdvSIMD DEBUG
  .NET 9.0 : .NET 9.0.8 (9.0.825.36511), Arm64 RyuJIT AdvSIMD
  ShortRun : .NET 9.0.8 (9.0.825.36511), Arm64 RyuJIT AdvSIMD

Runtime=.NET 9.0  

```
| Method            | Job      | Toolchain              | IterationCount | LaunchCount | WarmupCount | DataSize | ChangeCount | Mean       | Error      | StdDev    | Gen0    | Gen1    | Allocated |
|------------------ |--------- |----------------------- |--------------- |------------ |------------ |--------- |------------ |-----------:|-----------:|----------:|--------:|--------:|----------:|
| **Fused_JoinProject** | **.NET 9.0** | **Default**                | **Default**        | **Default**     | **Default**     | **100000**   | **1**           |   **3.059 μs** |  **0.0202 μs** | **0.0189 μs** |  **1.3351** |  **0.3357** |  **10.93 KB** |
| Fused_JoinProject | InProc   | InProcessEmitToolchain | Default        | Default     | Default     | 100000   | 1           |   3.068 μs |  0.0167 μs | 0.0148 μs |  1.3351 |  0.3357 |  10.93 KB |
| Fused_JoinProject | ShortRun | Default                | 3              | 1           | 3           | 100000   | 1           |   3.161 μs |  0.4145 μs | 0.0227 μs |  1.3351 |  0.3357 |  10.93 KB |
| **Fused_JoinProject** | **.NET 9.0** | **Default**                | **Default**        | **Default**     | **Default**     | **100000**   | **100**         | **394.525 μs** |  **3.5886 μs** | **3.1812 μs** | **66.4063** | **16.6016** | **546.24 KB** |
| Fused_JoinProject | InProc   | InProcessEmitToolchain | Default        | Default     | Default     | 100000   | 100         | 354.478 μs |  5.0931 μs | 4.7641 μs | 66.4063 | 16.6016 | 546.24 KB |
| Fused_JoinProject | ShortRun | Default                | 3              | 1           | 3           | 100000   | 100         | 388.843 μs | 28.0091 μs | 1.5353 μs | 66.4063 | 16.6016 | 546.24 KB |
