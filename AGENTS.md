# DBSP.NET Agent Handbook

*Last reviewed: 17 September 2025*

## Mission Brief
- DBSP.NET delivers an F# 9.0 implementation of the Database Stream Processor (DBSP) with a focus on incremental view maintenance and algebraic correctness.
- Treat this repository as an active research-and-development space: the implementation plan evolves alongside prototypes, benchmarks, and documentation.
- Maintain cross-references between code and `IMPLEMENTATION_PLAN.md`, `PERFORMANCE.md`, and `BENCHMARKING.md` whenever you expand or adjust capabilities.

## Repository Snapshot
- Primary language: F# (targeting .NET 9) with supporting PowerShell/Bash scripts for tooling.
- Core runtime composed of circuits, operators, storage, and diagnostics libraries under `src/`.
- Tests are split by concern under `test/` (unit, property-based, storage, and performance suites).
- Benchmarks and historical performance data live in `benchmark_results/` (raw runs) and `benchmark_analysis/` (processed summaries).
- Reference material under `source_code_references/` mirrors Rust (Feldera), Python, and tutorial implementations; use these to validate design choices.

## Key Paths
- `src/DBSP.Core` – Algebraic primitives, Z-set/Indexed Z-set types, core collections, and shared APIs.
- `src/DBSP.Operators` – Linear, join, recursive, aggregation, temporal, and fused operator implementations plus public operator API.
- `src/DBSP.Circuit` – Circuit builder, runtime, scheduler, optimizers (batch/span), parallel runtime, handles, and visualization helpers.
- `src/DBSP.Storage` – Storage abstractions, LSM/temporal traces, spilling strategy, serialization helpers, and the in-repo storage design notes.
- `src/DBSP.Diagnostics` – Circuit tracing, state validation, graph inspection, and monitoring utilities.
- `examples/DBSP.Examples` – Minimal console host wiring sample pipelines; keep this aligned with the latest public API.
- `test/DBSP.Tests.Unit` – Deterministic unit coverage for algebra/operator/circuit logic.
- `test/DBSP.Tests.Properties` – FsCheck property suites; extend here when adding algebraic laws or invariants.
- `test/DBSP.Tests.Storage` – Persistence and spill-over validation.
- `test/DBSP.Tests.Performance` – Smoke-level performance assertions to catch obvious regressions before full benchmarking.
- `scripts/benchmark` – Entry point script for running curated benchmark sets (invoked by CI and `run-phase5-1-benchmarks.sh`).
- `scripts/run-benchmark-analysis.{sh,ps1}` – Post-process benchmark output into `benchmark_analysis/` dashboards.
- `benchmark_results/` – Raw JSON/CSV benchmark captures grouped by date; never hand-edit.
- `worktrees/` – Auxiliary Git worktrees; leave untouched unless instructed.

## Build & Verification Workflow
- Install the .NET 9 SDK (`global.json` locks the required version) before building.
- Preferred sequence for local changes:
  1. `dotnet restore`
  2. `dotnet build --configuration Release`
  3. `dotnet test` (consider `--filter` for targeted runs while iterating)
- Always finish with full `dotnet test` runs; ensure builds and tests emit zero warnings.
- For performance-sensitive work (anything touching operators, circuits, or storage), run `./test-regression.sh` prior to completion and add performance coverage if absent.
- Capture noteworthy benchmark runs via `scripts/benchmark`; archive results under `benchmark_results/` and re-run analysis scripts when data changes.

## Development Priorities (2025 Roadmap)
- Phase 1 – Core Foundations: solidify algebraic abstractions, Z-set semantics, Indexed Z-set performance, and stream primitives.
- Phase 2 – Operators: complete linear/filter/flat-map suite, hash joins, aggregation set (SUM/COUNT/AVG), and delta-friendly union/minus.
- Phase 3 – Circuit Runtime: expand builder ergonomics, ensure single-threaded determinism, add input/output handles, and pursue operator fusion.
- Parallelization, storage durability, and diagnostics are current stretch goals; reference `PERFORMANCE.md` for target metrics and outstanding tasks before adding features.

## Working Style Expectations
- Favor immutable, composable F# patterns; use modules and explicit types to encode algebraic guarantees.
- Keep documentation synchronized: update `README.md`, `PERFORMANCE.md`, or benchmarking docs when behavior/metrics shift.
- When leveraging external references, prefer the `feldera/` Rust implementation for canonical behavior, the Python code for pedagogy, and the `dbsp-from-scratch/` material for sanity checks.
- Record assumptions and TODOs inline using concise comments; escalate larger design questions to `IMPLEMENTATION_PLAN.md` so they are tracked centrally.

## Exit Checklist for Any Change
- All `dotnet` builds/tests pass without warnings.
- `./test-regression.sh` executed (or documented why it was unnecessary) when touching hot paths.
- Benchmark artifacts updated or explicitly noted as unchanged.
- Documentation and examples reflect the final API surface.
- Confirm no incidental edits to generated artifacts (`bin/`, `obj/`, benchmark output files) land in commits.

Following this handbook keeps automated agents aligned with the DBSP.NET R&D roadmap while preserving performance and correctness guarantees.
