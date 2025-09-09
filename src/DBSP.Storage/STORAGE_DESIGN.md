# DBSP.Storage Design Notes

This document summarizes the storage layout and invariants for Phase 5.3.

KV layout
- Key: composite `(K,V)` stored as a single logical key for Z-set entries.
- Value: `int64` weight; positive = insert, negative = delete, 0 is elided.
- Rationale: matches DBSP’s Z-set semantics and enables weight-only updates without re-serializing values.

On-disk structure (target)
- Engine: ZoneTree LSM (one tree per worker). Current code uses an in-memory stand-in while interfaces settle.
- Directory: `${DataPath}/lsm/` per worker/runtime instance.
- Compaction: background compaction merges segments and removes zero-weight entries.

Serialization
- Interface: `DBSP.Storage.ISerializer<'T>` with `Serialize`, `Deserialize`, `EstimateSize`.
- Default: MessagePack with FSharpResolver via `SerializerFactory.GetDefault<'T>()`.
- ValueTuple interop: prefer `struct ('a * 'b)` in public APIs consumed by C#.

Temporal trace (target)
- Batches: immutable groups of `(K,V,weight)` entries, no time in keys.
- Trace/Spine: append batches at non-decreasing logical times; multi-level merge reduces read/merge cost; compaction eliminates zeros.
- Current: a minimal in-memory `TemporalSpine` to validate API shape and tests.

Invariants
- Weight aggregation: merging batches sums weights per `(K,V)`; weight 0 entries are dropped.
- Monotonic time: trace does not accept times < current frontier (enforced at API or caller level).
- Iteration order: by `(K,V)`; range queries bound by `K`.

Notes for contributors
- Keep F# ↔ C# interop in mind: use `struct` tuples for public APIs; expose class members (not only interfaces) when C# calls them directly.
- Avoid embedding time in storage keys; time lives in batch metadata (`TemporalSpine`).
- Keep write paths batched; prefer background compaction; avoid per-key synchronous commits.
