FastZSet Worktree Status
=========================

Branch: fastzset-experiment (worktree)
Path: worktrees/fastzset-experiment

Goals
- Make FastZSet viable end-to-end by addressing iteration and allocation overheads.
- Speed up benchmark turnaround for iteration and diagnostics.

Changes Implemented
- Iteration improvements:
  - Added an `Occupied` index list to `FastZSet` and switched `toSeq` to iterate O(count) via occupied indices rather than O(capacity) bucket scans.
  - Added compaction on resize and a `compact` helper that rebuilds the table and occupied list (removes tombstones).
- Enumerator/closure overhead:
  - `toSeq` now iterates occupied indices without per-bucket branching; still returns an `IEnumerable`. Next step would be a true struct enumerator plumbed through `ZSet.iter`/operators.
- Lookup operations:
  - Added `containsKey` and exposed it via `ZSet.containsKey`.
  - Replaced HashSet-based duplicate avoidance in `JoinMapOperator` with `ZSet.containsKey` for left-delta membership checks.
- Union tuning:
  - `union` now pre-sizes from `Count` and iterates via occupied indices to reduce scans/resizes.
- Bench runtime reductions (quick mode):
  - Env var `DBSP_QUICK_BENCH=1` enables a lighter job (InProc, 1 warmup, 5 iters).
  - Converted params to `ParamsSource` and, in quick mode, restrict `DataSize` to `[100k; 300k]` and `ChangeCount` to `[1; 100]`.
  - Removed attribute-based separate-process job from large-scale benches to avoid duplicate job runs.

Next Steps (Not Done Yet)
- Struct enumerator: implement a true struct enumerator + custom enumerable to minimize allocations and wire `ZSet.iter` to use it in hot paths (operators/aggregations).
- Adaptive compaction: trigger `compact` based on `Tombstones` ratio (e.g., > 25–33%).
- Optional shrinking: rehash down when `Count << Capacity / 2` for long-lived small deltas.
- Microbench coverage: add microbenches for `containsKey/tryGetWeight` and `unionMany` with delta-sized inputs.

How to Run Quick Benches
```bash
cd worktrees/fastzset-experiment
DBSP_QUICK_BENCH=1 dotnet run -c Release --project test/DBSP.Tests.Performance -- --filter "*LargeScalePipelineBenchmarks*|*CircuitLargeScaleBenchmarks*"
```

Notes
- This worktree is isolated from `main`. The primary working copy has been checked out to `main` as requested.

