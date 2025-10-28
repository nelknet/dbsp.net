# DBSP.NET Adaptive ZSet — Merge Strategy and Long‑Term Plan

## Summary

- Adopt a single adaptive `ZSet` implementation that internally combines:
  - A mutable memtable (FastZSet‑style) for O(1) point updates and fast ingestion.
  - An immutable Batch/Trace spine (sorted vectors + compaction) for join/scan locality and streaming consolidation.
- Keep things simple for users: one `ZSet` API, no modes to pick. Adaptation is automatic based on workload and operator needs.
- For merge safety, keep `main` default behavior identical (HashMap) while we land the adaptive engine behind an override.
- Preserve the existing `DBSP_BACKEND` override for A/B and debug: `HashMap`, `FastZSet`, `Batch`. Default behavior becomes “Adaptive” once parity gates are met.

## Why adaptive (and not one static structure)

- Conflicting goals: O(1) expected updates (hash) vs. linear, cache‑friendly scans/joins (sorted arrays) vs. long‑running consolidation (compaction). One static layout can’t dominate all three.
- Batch/Trace shines for arranged joins/aggregations and long‑lived streams, while a mutable hash excels at bursty point updates and unions.
- The best practical design is tiered: write into a fast memtable, periodically flush to sorted batches, and compact opportunistically — i.e., an LSM‑like spine specialized for Z‑sets.

## Adaptive ZSet design

Architecture

- Memtable: FastZSet‑style Robin‑Hood hash map with O(count) iteration, struct enumerator, `containsKey/tryFind`, `unionMany` pre‑size.
- Batches: immutable, sorted `(key, weight)` vectors supporting linear merge, negate/scalar, and consolidation.
- Trace: a small list of batches with size‑based levels; background compaction keeps a bounded number of batches with consolidated weights.
- Arranged views: when downstream operators (join/group/distinct) request arranged‑by‑key, the ZSet exposes the Trace; otherwise reads can prefer the memtable or a flat batch.

Signals (cheap, runtime + topology)

- Topology hints: number/type of arranged subscribers on this ZSet, by key layout.
- Update mix: tuples/s, bytes/s; scans/joins vs. point lookups.
- Cancellation rate: sum(|weights|) vs. consolidated size to detect churn that benefits from early flush/consolidation.
- Size/shape: cardinality, hot‑key skew, tombstones, small‑set thresholds.
- Budget/backpressure: compaction backlog, per‑tick time budget, latency goals.

Policies (defaults are conservative; tunable)

- Memtable flush: seal→sort→add batch when any hold:
  - Size S (e.g., ~64k entries or ~4–8 MiB),
  - Time T since last flush (e.g., 10–30 ms),
  - Join/scan demand is high (arranged subscribers active),
  - Cancellation rate exceeds threshold (high noise → consolidate sooner).
- Arrangement on demand: if arranged subscribers > 0, maintain a live Trace and flush more readily; if 0, stay memtable‑heavy and flush less aggressively.
- Compaction: LSM‑style levels with at most R batches per level (e.g., R=4). Spend up to B ms/tick (e.g., 1–2 ms) merging the smallest or most‑read batches.
- Tiny sets: keep ≤N entries (e.g., N=256–512) as a flat small vector; upgrade/downgrade based on size and churn.
- Popular‑key hint: optionally maintain a small side index for hot keys to speed probes into older batches when the probe/update ratio is high.

Mechanics

- On insert: update memtable; evaluate `should_flush(stats, subscribers)`; if true, seal→sort→append batch.
- On read (join/scan): if memtable is large and arranged view is needed, opportunistically flush to avoid expensive ad‑hoc merges.
- On tick: `compact_until(budget_ms)` to keep batch counts bounded and consolidate cancellations.
- Instrumentation: rolling EWMAs for rates and sizes; expose counters to perf tests and trace logs for tuning.

## Current Quick Benchmark Signals (directional)

Environment: QuickInProc (Warmup=1, Iterations=5), macOS/Apple Silicon; large‑scale suites only (representative picks):

- LargeScalePipeline (apply‑only and a simple pipeline)
  - Apply‑only (300k/100 changes): FastZSet ≈ 21.5 ms; HashMap ≈ 19.8 ms; Batch ≈ 56.4 ms.
  - Pipeline scan (300k/100 changes): FastZSet ≈ 112.7 ms; HashMap ≈ 200.8 ms; Batch ≈ 224.8 ms.
- CircuitLargeScale (true incremental pipeline with join/agg)
  - Incremental_Circuit (1M/100 changes): HashMap ≈ 0.33 ms; FastZSet ≈ 0.78 ms; Batch ≈ 0.90 ms.
  - FusedJoinProject (1M/100 changes): FastZSet/HashMap in the ~0.12–0.14 ms range; Batch ≈ 0.21 ms.

Interpretation:

- FastZSet shines on apply/union‑heavy and scan‑heavy paths.
- HashMap is still the fastest on pure join micro‑latencies in the circuit (today’s code).
- Batch/Trace aligns best with dbsp’s architecture and has the most long‑term potential with arranged state + background compaction.
- The adaptive design targets “best of both” by keeping updates in FastZSet until joins/arrangements make batches preferable, then compacts in the background.

## Parity Gates (before flipping default to Adaptive)

- CircuitLargeScale (100k/1M; ChangeCount 1/10/100): Incremental_Circuit and FusedJoinProject within ±5% of current `main` (HashMap) or better.
- LargeScalePipeline: Wins preserved (≥20–30%) under realistic delta distributions.
- Comparable or lower allocations on circuit macro‑benches.

## Merge Plan (clean, low risk)

1. Keep `HashMap` as default in `main` (unchanged behavior), and merge the worktree.
2. Land the adaptive engine behind the existing override. Short‑term, allow forcing a specific path with `DBSP_BACKEND={HashMap,Batch,FastZSet}`; medium‑term add `DBSP_BACKEND=Adaptive` and make it the default in the worktree.
3. Add property tests to assert cross‑backend semantic equivalence:
   - Random sequences of ZSet ops (add/negate/scalar/unionMany/filter/mapKeys) must produce the same logical result across HashMap, Batch, FastZSet, and Adaptive.
4. Add CI matrix for quick benchmarks and tests:
   - `DBSP_BACKEND={HashMap,Batch,FastZSet,Adaptive}`; `DBSP_QUICK_BENCH=1` for macro‑bench smoke.
5. Documentation updates (README + FAST_ZSET_STATUS.md):
   - Adaptive design, key constraints (`'K: comparison`), tuning knobs (S/T/R/B/N), quick run commands.

## Roadmap to “No Compromises”

Short term (post‑merge):

- Adaptive core:
  - Implement memtable→batch flush path with size/time/cancellation thresholds (S/T) and arranged‑on‑demand hints.
  - Add background compaction with a per‑tick budget (B) and level cap (R).
  - Tiny‑set small‑vector fallback (≤N) with auto upgrade/downgrade.
- Batch/Trace:
  - Formalize arranged indexes for join/group and reuse across deltas.
  - Improve linear merge specialization and SIMD‑friendly kernels.
- FastZSet:
  - Remove iteration snapshot allocations (versioned/non‑alloc iteration) or amortize via reusable snapshots.
  - Continue threshold tuning (tombstone/shrink) guided by microbenches.
- Systemic:
  - Ensure all hot operators output single builds (`buildWith`); avoid per‑insert loops.
  - Introduce `ZSet.freeze()/toArray` where read‑mostly repetition occurs within a single evaluation.

Long term:

- Make Adaptive the canonical default once parity gates are met.
- Keep explicit `HashMap`/`Batch`/`FastZSet` overrides only for A/B and diagnostics.

## Operational Notes

- Backend selection:
  - `DBSP_BACKEND=HashMap` — current behavior on `main` (merge‑safe default).
  - `DBSP_BACKEND=Batch` — dbsp‑style immutable batches (trace/compaction).
  - `DBSP_BACKEND=FastZSet` — mutable Robin‑Hood hash (optimized ingestion/union).
  - `DBSP_BACKEND=Adaptive` — unified engine (memtable + trace); becomes the default after parity gates.
- Quick macro‑bench runs:
  - `DBSP_QUICK_BENCH=1 dotnet run -c Release --project test/DBSP.Tests.Performance -- --filter "*LargeScalePipelineBenchmarks*|*CircuitLargeScaleBenchmarks*"`
- Representative A/B:
  - `DBSP_BACKEND=HashMap DBSP_QUICK_BENCH=1 ...`
  - `DBSP_BACKEND=Batch DBSP_QUICK_BENCH=1 ...`
  - `DBSP_BACKEND=FastZSet DBSP_QUICK_BENCH=1 ...`
  - `DBSP_BACKEND=Adaptive DBSP_QUICK_BENCH=1 ...`

## Risks & Mitigations

- Adaptation thrash (too‑eager flush/compact):
  - Use hysteresis on thresholds; EWMA‑based decisions; cap work per tick (B).
- Compaction backlog growth:
  - Prioritize merges for hot/queried batches; raise S/T under sustained load; surface metrics.
- Join micro‑latency regressions:
  - Prefer arranged views when subscribers exist; opportunistic flush before joins; keep tiny overlays in memtable.
- Allocation overhead in FastZSet.iter:
  - Mitigate with versioned snapshots or non‑alloc iteration; already on the roadmap.
- Complexity creep:
  - Single `ZSet` API; adaptive behavior is internal. Internally, represent payload as a DU (exactly one of memtable/small‑vector/trace) to keep footprint low.

## Acceptance Criteria (to flip default to Adaptive)

- CircuitLargeScale parity (±5% on target cases).
- LargeScalePipeline wins preserved (≥20–30%).
- Allocation parity in macro‑benches.
- Documentation updated; CI green across backends.

---

Prepared in `worktrees/fastzset-experiment` for merge readiness. The default on `main` should remain HashMap initially; Adaptive will be introduced behind the existing `DBSP_BACKEND` override and made the default after parity gates are met. Explicit `HashMap`/`Batch`/`FastZSet` remain available for A/B testing and diagnostics.

## Implementation Status (Current Worktree)

- Implemented
  - Single `ZSet` API with Adaptive backend behind `DBSP_BACKEND=Adaptive`; default remains `HashMap`.
  - Memtable (FastZSet), SmallVec for tiny sets, and immutable Batch/Trace spine.
  - K‑way arranged iterator over Trace (no full consolidation), used by reads.
  - Normalization and flush for Adaptive: equality and iteration compare the logically consolidated view; `iter`/`toSeq`/`Keys`/`Count` normalize/flush when needed.
  - Non‑alloc fold paths based on `iter` to reduce short‑lived allocations.
  - Batch/Trace read path uses a cached consolidated array for repeated scans; cache is invalidated by `addBatch`/`union`/`negate`/`scalar`.
  - IndexedZSet builders for `groupBy`/`fromZSet`/`ofSeq`; join rewritten to use builder + streaming `iter` products.
  - JoinOperators: Adaptive fast path in inner join using per‑key mutable dictionaries and grouped deltas.
  - All unit, property, and storage tests pass under Adaptive in quick runs.
  - Quick benchmark slices run under Adaptive; regression harness shows no quick‑mode regressions.
- Partially implemented
  - Compaction: simple level cap (fan‑out) implemented; budgeted background compaction (time‑boxed) not yet wired.
  - Operator integration: join/group/distinct still read via generic iteration; explicit arranged view handoff is planned next.
- Not yet wired
  - Tuning via env vars (e.g., `DBSP_ZSET_FLUSH_*`, `DBSP_ZSET_LEVEL_*`, `DBSP_ZSET_COMPACT_*`): currently using conservative in‑code defaults except `DBSP_BACKEND`.
  - Subscriber‑aware flush policy and EWMA‑based heuristics; metrics registry for live tuning.

### Implemented Changes (Worktree)

- Core modules
  - `src/DBSP.Core/ZSet.fs`
    - Added `normalizeAdaptiveInternal` and `flushAdaptiveInternal` to create/maintain arranged batches.
    - Ensured `iter`/`toSeq`/`Keys`/`Count` flush/normalize before arranged reads; equality compares logical consolidated view.
    - Added non‑alloc folding paths over struct iterators for hot loops.
    - Plumbed environment‑based tuning stubs (`DBSP_ZSET_*`) for future policy hooks.
  - `src/DBSP.Core/BatchTrace.fs`
    - Replaced eager full consolidation per read with a k‑way arranged iterator and a cached consolidated array for repeated scans.
    - Invalidated cache on mutations: `addBatch`/`union`/`negate`/`scalar`.
  - `src/DBSP.Core/IndexedZSet.fs`
    - Switched `groupBy`/`fromZSet`/`ofSeq` to builder‑based per‑key construction to minimize allocations.
    - Rewrote `join` to stream products using builders and `iter` rather than materializing intermediates.
  - `src/DBSP.Operators/JoinOperators.fs`
    - Added an Adaptive fast path in `InnerJoinOperator` using per‑key mutable overlays and grouped deltas; retained generic `IndexedZSet` path.

### Benchmark Summary (Quick Mode)

- LargeScalePipeline
  - Apply‑only 300k/100: Adaptive near HashMap; both far ahead of Batch.
  - Pipeline scan 300k/100: Adaptive achieves large wins versus HashMap/Batch due to arranged scans and caching.
- CircuitLargeScale
  - Fused join/project 1M/100: Adaptive competitive with HashMap; Batch behind.
  - Incremental heavy delta 1M/100: Adaptive slower than HashMap (Adaptive ≈ 2.1 ms vs HashMap sub‑ms in our runs), indicating room for a specialized heavy‑delta path.

Note: Exact numbers vary by run and tuning; see `benchmark_results/*/DBSP.Tests.Performance.*-report-*.{md,csv}` in this worktree.

### Open Issues / Next Steps

- Heavy‑delta circuit join case still lags HashMap; pursue per‑key overlays and fused join+project with lazy normalization.
- Formalize `ArrangedView<'K,'V>` handle and let operators consume it directly; remove ad‑hoc arranged reads.
- Subscriber‑aware flush and budgeted compaction with hysteresis to avoid thrash.
- Evaluate hashed‑sorting/radix compaction for very large batches and skewed keys.

## Detailed Design

Types and constraints (F#-centric)

- Key: `'K : comparison` for ordering in batches/trace; hash and compare both available.
- Weight: `'W :> IGroup<'W>` (Group) and optionally `IRing<'W>` for scalar ops; default `int` weight.
- ZSet payload as a discriminated union to enforce exactly-one active layout:
  - `SmallVec of struct ('K * 'W) array * int` — compact small-set representation (length ≤ N, sorted by `'K`).
  - `Memtable of FastZSet<'K,'W>` — mutable Robin-Hood hash map with struct enumerator.
  - `Trace of TraceSpine<'K,'W>` — immutable sorted batches with leveled compaction.
- Public API remains `ZSet<'K,'W>` with methods: `add`, `addMany`, `negate`, `scale`, `mapKeys`, `filter`, `union`, `unionMany`, `iter`, `tryFind`, `count`, `arrangedView`.

Key invariants

- SmallVec is always sorted and consolidated (no duplicate keys, weights combined; zero weights elided).
- Each Batch in Trace is sorted by `'K` and internally consolidated.
- Trace levels uphold size monotonicity: total batches per level ≤ R; upper level batches ≥ c × lower level size.
- Memtable may contain tombstones; reads consolidate on the fly only if necessary and bounded by a per-iterator budget.
- All public operations are logically equivalent across payload variants.

Algorithms (pseudocode)

- Memtable insert
  - `put(k,w)`:
    - `fastZSet.upsert(k, +w);` if post-upsert weight==0, mark tombstone or remove if cheap.
    - `if should_flush(stats, subscribers) then flush()`.

- Flush (seal → batch)
  - `flush()`:
    - `let snapshot = fastZSet.takeAndReset()` (O(1) swap) → iterator of `(k,w)`.
    - Copy to vector, `Array.sortInPlaceBy key`, consolidate equals, drop zeros → `Batch`.
    - `trace.append(batch); stats.lastFlush <- now()`.

- Compaction scheduler (per tick)
  - `compact_until(budget_ms)`:
    - while `budget > 0 && exists level with > R batches`:
      - pick victim pair by heuristic (smallest, hottest, or oldest).
      - `merge_linear(b1,b2) -> b'` with consolidation; place into next level.
      - decrement `budget` by measured cost; update metrics.

- Read path
  - `iter()`:
    - if `SmallVec` → return its struct enumerator.
    - if `Memtable` only → iterate hashtable entries via struct enumerator.
    - if `Trace` present → return a multi-iterator that merges `Memtable` snapshot (optionally partial) with batches if caller needs arranged order; else iterate memtable then batches.
  - `arrangedView()`:
    - ensure `Trace` exists (trigger opportunistic `flush` if memtable is large); return a handle over the Trace spine for arranged consumers.

Complexities (amortized)

- Point update: O(1) expected in `Memtable`.
- Flush: O(M log M) sort, O(M) consolidate; amortized by S/T thresholds.
- Linear merge between batches: O(|b1| + |b2|), cache-friendly.
- Join/Group on arranged inputs: O(|A| + |B|) per level merge + probe cost bounded by batch count (kept small by compaction).

Configuration and tuning

- Environment variables (read at startup; overridable via code):
  - `DBSP_BACKEND`: `HashMap | Batch | FastZSet | Adaptive` (default `HashMap` until parity gates, then `Adaptive`).
  - `DBSP_ZSET_SMALLSET_N` (N): small-set max size (default 512).
  - `DBSP_ZSET_FLUSH_SIZE` (S): memtable flush target entries (default 64k).
  - `DBSP_ZSET_FLUSH_BYTES`: alternative byte budget (default 8 MiB).
  - `DBSP_ZSET_FLUSH_TIME_MS` (T): time since last flush (default 20 ms).
  - `DBSP_ZSET_LEVEL_FANOUT` (R): max batches per level (default 4).
  - `DBSP_ZSET_COMPACT_BUDGET_MS` (B): per-tick compaction budget (default 2 ms).
  - `DBSP_ZSET_CANCEL_EWMA`: cancellation rate threshold fraction (default 0.25 of size).
  - `DBSP_ZSET_LOGGING`: metrics verbosity (off|summary|detailed).

Instrumentation and metrics

- Per-ZSet counters: inserts/sec EWMA, bytes/sec, flush count, avg flush size, time since last flush, batches per level, compaction ms/tick, backlog size, iterator merges, arranged subscribers.
- Expose via a lightweight registry and dump on demand or at benchmarks end.
- Integrate with existing `test-regression.sh` perf summary to surface regressions.

Testing plan

- Property-based (FsCheck):
  - Equivalence across backends for random sequences of ops.
  - Invariance of consolidation: `z.add(x, w).add(x, -w) == z`.
  - Distributivity: `mapKeys(f, z1 ∪ z2) == mapKeys(f, z1) ∪ mapKeys(f, z2)`.
  - Associativity/commutativity under addition for commutative groups.
- Unit tests:
  - SmallVec transitions around threshold N (upgrade/downgrade) without semantic change.
  - Flush triggers on S/T and cancellation EWMA.
  - Compaction preserves order and consolidation; levels invariant.
- Integration tests:
  - CircuitLargeScale under all backends; assert result equivalence and latency envelopes.
  - LargeScalePipeline deltas; assert wins do not regress beyond budget.
- CI matrix: run tests with `DBSP_BACKEND` in `{HashMap,Batch,FastZSet,Adaptive}`; quick macro-bench gated by `DBSP_QUICK_BENCH=1`.

Benchmark suite additions

- Microbenches:
  - Point updates (random/sequential/hot-spot) for HashMap vs FastZSet.
  - Flush throughput vs S/T; compaction cost vs R.
  - Iteration/scan over sizes for Memtable/Trace/SmallVec.
  - Join on arranged vs non-arranged inputs, varying batch counts.
- Macrobenches:
  - Existing CircuitLargeScale and LargeScalePipeline; add scenarios with high cancellation rates and hot-key skew.

Integration plan (code touchpoints)

- `src/DBSP.Core/ZSet.fs`: introduce `AdaptiveZSet<'K,'W>` DU payload; keep existing `ZSet` API delegating to payload.
- `src/DBSP.Core/IndexedZSet.fs`: arranged view trait/handle to expose Trace.
- `src/DBSP.Core/Algebra.fs`: ensure `IGroup`/`IRing` contracts are explicit for `'W`.
- `src/DBSP.Operators/*`: joins/aggregations consume arranged view when available; otherwise operate over iterator abstraction.
- Backend selector: central factory honoring `DBSP_BACKEND` to instantiate HashMap/Batch/FastZSet/Adaptive.

Migration and compatibility

- No public API break: same `ZSet` surface; new behavior is internal.
- Serialization (if present) remains key/weight sequence; payload layout is not serialized.
- Deterministic iteration when arranged view is requested; otherwise iteration order is unspecified as today.

Failure modes and safeguards

- Memory pressure: raise S/T thresholds; slow compaction budget B adaptively; expose backpressure metric.
- Excessive compaction backlog: cap per-level batches and elevate compaction priority for hottest levels.
- Thrash around thresholds: apply hysteresis (e.g., N±δ, S±δ) and cool-down windows post-transition.

Recommendations and forward plan

- Keep HashMap as the default; keep Adaptive behind `DBSP_BACKEND=Adaptive`.
- Target DBSP‑typical wins with Adaptive:
  - Operator‑level arranged view reuse: joins, group‑bys, aggregations consume arranged iterators/handles; avoid rebuilding per tick.
  - Budgeted compaction and subscriber‑aware flush: if arranged subscribers exist, flush more readily and compact under a small per‑tick budget; otherwise prefer memtable‑only.
  - Heavy‑delta hot path: add per‑key mutable overlays and fuse join+project to cut product‑emission cost; normalize lazily and under budget.
- Simple adaptive policy (no mode swaps visible to users):
  - memtable‑only when arranged_subscribers=0 and delta EWMAs are large; arranged mode when subscribers>0 or arrangedView is requested.
  - Use hysteresis (enter/exit windows) to avoid oscillation; pre‑seed arranged cache on entry.
- Instrumentation and controls:
  - Expose flush/compaction budgets, EWMAs, batch counts, cache hits, and subscriber counts; knobs via env vars: `DBSP_ZSET_FLUSH_*`, `DBSP_ZSET_LEVEL_FANOUT`, `DBSP_ZSET_COMPACT_BUDGET_MS`.
- Validation:
  - Property tests across backends (semantic equivalence), policy unit tests (flip/hysteresis), operator tests (arranged vs generic), benchmark slices (CircuitLargeScale, LargeScalePipeline) with latency and allocation tracking.
- Optional: hashed‑sorting compaction experiment for very large batches to stabilize consolidation under skew (hash keys with an invertible hasher, then radix‑sort during compaction/build).

Open questions

- How aggressively should joins trigger opportunistic flush under tight latency SLOs?
- Do we need a small hot-key index, or will compaction suffice for probe locality?
- Should we expose per-operator hints to bias S/T/R/B on operator types (e.g., joins vs unions)?
- What bounded on-iterator consolidation budget feels right for worst-case tombstones?

## Post-Implementation Benchmark Update

Implemented in this worktree:
- ArrangedView handle and subscriber-aware flush thresholds (lower S/T when arranged consumers exist).
- Budgeted compaction (`DBSP_ZSET_COMPACT_BUDGET_MS`) and cached consolidated arrays for repeated scans.
- Hashed-bucketing + k-way merge in batch normalization for very large inputs to cut comparisons under skew.
- IndexedZSet.toZSet reworked to a single builder; fewer allocations on join paths.
- InnerJoinOperator Adaptive fast path (per-key mutable overlays, grouped deltas) retained and tuned.

Quick results (ShortRun, local M4 Max; see benchmark_results timestamped directories):
- Join microbench (OperatorBenchmarks.InnerJoin_Incremental)
  - SetSize=100: Adaptive ≈ 626 μs; HashMap ≈ 262 μs
  - SetSize=1000: Adaptive ≈ 10.66 ms; HashMap ≈ 4.23 ms
  - SetSize=5000: Adaptive ≈ 64.21 ms; HashMap ≈ 40.62 ms
  - Conclusion: Adaptive improved but still trails HashMap for small-state, join-heavy deltas.
- LargeScalePipeline (previous quick runs in repo): Adaptive continues to show large wins for arranged scans; apply-only is near HashMap.

Parity status: Not yet at parity for heavy-delta join cases. Next steps target deeper specialization (e.g., fused product emission for primitives, stricter small-set paths) and extending arranged handle consumption across operators to avoid per-call normalization.
