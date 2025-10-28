```

BenchmarkDotNet v0.15.2, macOS Sequoia 15.6.1 (24G90) [Darwin 24.6.0]
Apple M4 Max, 1 CPU, 16 logical and 16 physical cores
.NET SDK 9.0.304
  [Host]   : .NET 9.0.8 (9.0.825.36511), Arm64 RyuJIT AdvSIMD DEBUG
  ShortRun : .NET 9.0.8 (9.0.825.36511), Arm64 RyuJIT AdvSIMD


```
| Method                | Job      | Toolchain              | IterationCount | LaunchCount | WarmupCount | SetSize | Mean        | Error       | StdDev    | StdErr    | Min         | Max         | Q1          | Q3          | Median      | Op/s     | Gen0       | Gen1      | Gen2     | Allocated |
|---------------------- |--------- |----------------------- |--------------- |------------ |------------ |-------- |------------:|------------:|----------:|----------:|------------:|------------:|------------:|------------:|------------:|---------:|-----------:|----------:|---------:|----------:|
| **InnerJoin_Incremental** | **InProc**   | **InProcessEmitToolchain** | **Default**        | **Default**     | **Default**     | **100**     |    **492.0 μs** |     **4.29 μs** |   **4.02 μs** |   **1.04 μs** |    **485.2 μs** |    **501.0 μs** |    **489.5 μs** |    **494.6 μs** |    **491.1 μs** | **2,032.39** |   **165.5273** |   **53.2227** |        **-** |   **1.32 MB** |
| InnerJoin_Incremental | ShortRun | Default                | 3              | 1           | 3           | 100     |    448.6 μs |    24.66 μs |   1.35 μs |   0.78 μs |    447.2 μs |    449.9 μs |    447.9 μs |    449.2 μs |    448.5 μs | 2,229.34 |   165.5273 |    6.8359 |        - |   1.32 MB |
| **InnerJoin_Incremental** | **InProc**   | **InProcessEmitToolchain** | **Default**        | **Default**     | **Default**     | **1000**    |  **8,412.0 μs** |    **88.63 μs** |  **82.91 μs** |  **21.41 μs** |  **8,257.5 μs** |  **8,590.2 μs** |  **8,349.1 μs** |  **8,456.1 μs** |  **8,398.7 μs** |   **118.88** |  **1812.5000** |  **562.5000** | **156.2500** |  **14.57 MB** |
| InnerJoin_Incremental | ShortRun | Default                | 3              | 1           | 3           | 1000    |  7,568.4 μs |   325.11 μs |  17.82 μs |  10.29 μs |  7,548.1 μs |  7,581.5 μs |  7,561.8 μs |  7,578.5 μs |  7,575.5 μs |   132.13 |  1812.5000 |  656.2500 | 187.5000 |   14.5 MB |
| **InnerJoin_Incremental** | **InProc**   | **InProcessEmitToolchain** | **Default**        | **Default**     | **Default**     | **5000**    | **53,815.9 μs** |   **540.13 μs** | **478.81 μs** | **127.97 μs** | **52,954.0 μs** | **54,630.1 μs** | **53,527.1 μs** | **54,104.4 μs** | **53,860.5 μs** |    **18.58** | **10250.0000** | **2562.5000** | **875.0000** |  **74.97 MB** |
| InnerJoin_Incremental | ShortRun | Default                | 3              | 1           | 3           | 5000    | 50,686.3 μs | 9,256.95 μs | 507.40 μs | 292.95 μs | 50,299.7 μs | 51,260.8 μs | 50,399.0 μs | 50,879.5 μs | 50,498.3 μs |    19.73 | 10300.0000 | 2600.0000 | 900.0000 |  75.05 MB |
