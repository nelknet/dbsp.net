# DBSP.Storage Design

This document details the storage layer used by DBSP.NET for persisting Z-set updates efficiently and correctly under incremental workloads.

## Objectives

- Durability and correctness: persist Z-set updates ((K,V) -> weight:int64) with algebraic semantics (addition; delete on zero).
- Low-latency updates: handle small incremental batches without heavy per-record overhead.
- Efficient scans: support full and range scans by K; predictable memory usage.
- Pluggable serialization: MessagePack-based, cross-language friendly.

## Architecture Overview

- InMemoryStorageBackend<'K,'V>:
  - Backed by `HashMap<'K, 'V * int64>` (FSharp.Data.Adaptive), used for development/tests.
  - Applies weight addition rules; zero weight removes the pair.

- LSMStorageBackend<'K,'V>:
  - Built on Tenray.ZoneTree (LSM). Storage key is composite `KV{ K; V }` with K-primary, V-secondary ordering; value is `int64` weight.
  - Comparer orders by K then V for contiguous key scans and range queries.
  - Serializers bridge our pluggable `ISerializer<struct ('K * 'V)>` to ZoneTree’s serializers.

- SerializerFactory:
  - Provides default MessagePack serializers (standard and LZ4-compressed) and an override hook for custom serializers per type.

- SpillCoordinator / AdaptiveStorageManager:
  - Surfaces memory pressure and hooks for spill decisions; current manager is minimal and returns an in-memory backend.

## Data Model and Semantics

- Z-set entry: tuple `(k:'K, v:'V, w:int64)` with `'K,'V : comparison`.
- Aggregation by key-value:
  - Within a batch: updates with the same `(k,v)` are coalesced by summing weights (see optimization below).
  - Across the store: `Upsert` applies the algebraic addition; if resulting weight is `0L`, the `(k,v)` entry is deleted.

## Write Path (LSM)

1. Batch coalescing (optimization): incoming `seq<(K*V*int64)>` is first grouped by `(K,V)` and weights summed. Zero-sum entries are dropped.
2. For each coalesced `(k,v,w)`:
   - TryGet existing weight; compute `newW = existing + w`.
   - If `newW = 0L` → delete; else `Upsert(newW)`.
3. Stats updated for written bytes and key count (approximate for now).

Rationale: Coalescing reduces ZoneTree writes and delete churn for bursts of updates containing repeated `(K,V)` keys, improving throughput and write amplification.

## Read Path (LSM)

- Get(k): returns the first `(k, v, w)` encountered for `K = k`. Tests do not depend on the chosen `v` value; this is primarily a convenience/readiness probe.
- GetIterator(): forward scan over ZoneTree iterator, yielding `(k,v,w)` for non-zero weights.
- GetRangeIterator(start,end): forward scan filtered to `K ∈ [start,end]` using the composite-key order.

Future work: Use ZoneTree lower-bound seek to jump directly to the first `K >= start` to avoid scanning from the beginning on range queries and single-key gets.

## Compaction

- Triggers via ZoneTree maintenance API: move mutable forward and merge until no in-memory records remain.
- Stats: increments `CompactionCount` and stamps `LastCompactionTime`.

## Serialization

- MessagePack with a composite resolver (Standard, FSharp, Contractless) to support tuples/records and interop.
- LZ4-compressed variant provided for larger values where CPU trade-off is favorable.

## Configuration (StorageConfig)

- DataPath: base directory for on-disk state.
- CompactionThreshold: used to size disk segments / memtable merge cadence.
- WriteBufferSize, BlockCacheSize: fed into ZoneTree disk segment options (heuristic cache size tuning).
- SpillThreshold: fraction of available memory that triggers spill decisions (where applicable).

## Invariants

- Weight addition is associative and commutative per `(K,V)`; zero weight entries are not materialized.
- Iterators never yield zero-weight entries.
- Compaction preserves logical contents (modulo zero-weight elision).

## Performance Notes & Tuning

- Batch coalescing: reduces write amplification when updates contain duplicates.
- Iterator usage: prefer lower-bound seeks (future) to avoid O(n) prefix scans.
- Cache sizing: `BlockCacheSize` split heuristically to key/value caches; adjust based on working set and access pattern.
- Serialization: consider compressed serializer for large `V`; for small `V`, standard serializer avoids extra CPU.

## Testing & Benchmarks

- Unit tests: CRUD, iteration, and range coverage.
- Property tests: algebraic properties (order-independence, idempotent compaction, no zero-weight leakage).
- Performance tests: batch insert throughput and scan rates; ensure no regressions via `./test-regression.sh`.
