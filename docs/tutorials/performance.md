# Performance Optimization

Once you are comfortable building circuits, the next step is to keep them fast.
This tutorial covers a pragmatic workflow for measuring and tuning DBSP.NET
pipelines.

Supporting code: [`examples/DBSP.Tutorials/Performance.fs`](../../examples/DBSP.Tutorials/Performance.fs)
provides a compact harness that benchmarks a miniature circuit and illustrates
how to compare naive vs incremental paths.

## 1. Start with a repeatable micro-benchmark

```bash
dotnet run --project examples/DBSP.Tutorials -- --sample performance --iterations 5
```

The harness prints timing summaries for both the naive implementation and the
incremental circuit. By default it uses the `Stopwatch` API from .NET to keep the
dependency footprint small.

## 2. Scale out with the built-in regression script

For larger changes (anything touching `src/DBSP.*`), lean on the repository
performance guardrail:

```bash
./test-regression.sh
```

The script drives curated BenchmarkDotNet suites and keeps historical results in
`benchmark_results/`. Update or add new scenarios whenever you introduce notable
pipelines.

## 3. Tune incrementally

The tutorial harness exposes a few techniques you can reuse:

- toggle the `DBSP_BACKEND` environment variable to compare Adaptive vs HashMap
- sweep batch sizes to confirm change-proportional scaling
- export the per-step deltas to ensure algorithms behave as expected

```bash
DBSP_BACKEND=HashMap dotnet run --project examples/DBSP.Tutorials -- --sample performance
```

## 4. Document findings

Finally, record the before/after numbers in `PERFORMANCE.md` or the relevant
proposal. Historical context is essential when reviewing future regressions.

### Additional resources

- `examples/DBSP.Examples/Program.fs`: a comprehensive naive vs incremental demo
- `source_code_references/feldera/`: canonical behaviour from the Rust engine
- `benchmark_analysis/`: processed BenchmarkDotNet reports for recent runs
