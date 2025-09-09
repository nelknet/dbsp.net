```

BenchmarkDotNet v0.15.2, macOS Sequoia 15.6.1 (24G90) [Darwin 24.6.0]
Apple M4 Max, 1 CPU, 16 logical and 16 physical cores
.NET SDK 10.0.100-preview.7.25380.108
  [Host] : .NET 9.0.8 (9.0.825.36511), Arm64 RyuJIT AdvSIMD DEBUG
  Dry    : .NET 9.0.8 (9.0.825.36511), Arm64 RyuJIT AdvSIMD

Job=Dry  IterationCount=1  LaunchCount=1  
RunStrategy=ColdStart  UnrollFactor=1  WarmupCount=1  

```
| Method                 | OperationCount | Mean     | Error | Completed Work Items | Lock Contentions | Allocated  |
|----------------------- |--------------- |---------:|------:|---------------------:|-----------------:|-----------:|
| **ZSet_BatchConstruction** | **1000**           | **3.396 ms** |    **NA** |                    **-** |                **-** |  **171.86 KB** |
| **ZSet_BatchConstruction** | **10000**          | **4.737 ms** |    **NA** |                    **-** |                **-** | **1718.73 KB** |
