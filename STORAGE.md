# DBSP.NET Storage System Implementation Plan

## Current Status

The storage module for DBSP.NET has been partially implemented but has compilation errors that need to be resolved. This document outlines the current implementation, issues, and the path forward to completion.

## Architecture Overview

### Core Components

1. **Storage Backends**
   - `InMemoryStorageBackend`: HashMap-based in-memory storage
   - `LSMStorageBackend`: Persistent storage using ZoneTree (currently stubbed)
   - `HybridStorageBackend`: Combines memory and disk storage with spilling

2. **Serialization**
   - `ISerializer<T>`: Pluggable serialization interface
   - `MessagePackSerializer`: Default serializer implementation
   - `SerializerFactory`: Factory pattern for serializer creation

3. **Memory Management**
   - `IMemoryMonitor`: GC pressure monitoring interface
   - `AdaptiveStorageManager`: Manages storage backends and memory pressure
   - Automatic spilling when memory threshold exceeded

4. **Temporal Storage**
   - `TemporalSpine`: Time-versioned storage for historical queries
   - Multi-level batch compaction
   - Frontier advancement for time management

## Technologies and Libraries

### Current Dependencies
- **FSharp.Data.Adaptive** (v1.2.25): Provides HashMap for O(1) operations
- **ZoneTree** (v1.8.5): LSM tree implementation for persistent storage
- **MessagePack.FSharpExtensions** (v4.0.0): F# serialization support
- **System.Collections.Immutable** (v9.0.9): Additional immutable collections

### Key Data Structures
- **HashMap<'K, 'V>**: From FSharp.Data.Adaptive for high-performance key-value storage
- **Z-set semantics**: Tuples of (key, value, weight) supporting addition/cancellation
- **Task-based async**: F# Tasks for async operations (not F# async workflows)

## Current Issues

### Compilation Errors (44 total)

1. **Type Constraint Issues**
   - Missing `'V: equality` constraints on generic type parameters
   - Affects: InMemoryStorageBackend, LSMStorageBackend, HybridStorageBackend, TemporalSpine

2. **Method Signature Mismatches**
   - `GetRangeIterator` implementation doesn't match interface signature
   - Incorrect parameter passing: passing tuple instead of curried parameters

3. **API Errors**
   - `GC.GetMemoryInfo()` should be `GC.GetGCMemoryInfo()`
   - Incorrect memory info property access

4. **Package Conflicts**
   - Duplicate FSharp.Core package references causing warnings

## Implementation Tasks

### Phase 1: Fix Compilation Errors
**Priority: Critical | Estimated: 2-3 hours**

#### Task 1.1: Fix Type Constraints
```fsharp
// Add equality constraint to all storage backend types
type InMemoryStorageBackend<'K, 'V when 'K : comparison and 'V : equality>
```

#### Task 1.2: Fix Method Signatures
```fsharp
// Fix GetRangeIterator to use curried parameters
member _.GetRangeIterator startKey endKey = 
    // instead of: member _.GetRangeIterator(startKey, endKey)
```

#### Task 1.3: Fix GC API Usage
```fsharp
// Correct GC memory info access
let info = GC.GetGCMemoryInfo()
float info.HeapSizeBytes / float info.HighMemoryLoadThresholdBytes
```

#### Task 1.4: Clean Package References
- Remove duplicate FSharp.Core references
- Ensure consistent package versions

### Phase 2: Complete LSM Storage Implementation
**Priority: High | Estimated: 4-6 hours**

#### Task 2.1: ZoneTree Integration
- Implement proper ZoneTree serializers for F# types
- Create key/value comparers for ZoneTree
- Handle ZoneTree configuration and options

#### Task 2.2: Persistent Storage Operations
- Implement actual file-based storage
- Add write-ahead logging (WAL) support
- Implement crash recovery mechanisms

#### Task 2.3: Compaction Strategy
- Implement LSM compaction triggers
- Add multi-level compaction logic
- Optimize merge operations for Z-sets

### Phase 3: Memory Management Enhancement
**Priority: Medium | Estimated: 3-4 hours**

#### Task 3.1: Adaptive Spilling
- Implement threshold-based spilling triggers
- Add hot/cold data classification
- Optimize spill/load operations

#### Task 3.2: Memory Monitoring
- Implement proper GC pressure callbacks
- Add memory usage statistics
- Create memory pressure events

### Phase 4: Temporal Storage Completion
**Priority: Medium | Estimated: 4-5 hours**

#### Task 4.1: Spine Structure
- Implement proper multi-level spine batches
- Add efficient batch merging
- Optimize time-range queries

#### Task 4.2: Compaction and Frontier
- Implement time-based compaction
- Add frontier advancement logic
- Handle late-arriving data

### Phase 5: Testing and Validation
**Priority: High | Estimated: 6-8 hours**

#### Task 5.1: Unit Tests
- Fix existing test compilation
- Add missing test coverage:
  - Concurrent access tests
  - Memory pressure tests
  - Crash recovery tests
  - Performance benchmarks

#### Task 5.2: Integration Tests
- End-to-end storage operations
- Multi-backend coordination
- Temporal query validation

#### Task 5.3: Performance Testing
- Benchmark against DBSP crate
- Memory usage profiling
- Throughput measurements

## Implementation Order

1. **Immediate (Day 1)**
   - Fix all compilation errors (Tasks 1.1-1.4)
   - Get basic storage module building

2. **Short Term (Days 2-3)**
   - Complete LSM storage implementation (Tasks 2.1-2.3)
   - Fix and run existing tests

3. **Medium Term (Days 4-5)**
   - Enhance memory management (Tasks 3.1-3.2)
   - Complete temporal storage (Tasks 4.1-4.2)

4. **Final Phase (Days 6-7)**
   - Comprehensive testing (Tasks 5.1-5.3)
   - Performance optimization
   - Documentation

## Success Criteria

### Minimum Viable Product
- [ ] Storage module compiles without errors or warnings
- [ ] Basic CRUD operations work correctly
- [ ] Z-set weight cancellation functions properly
- [ ] All unit tests pass

### Production Ready
- [ ] ZoneTree integration complete with persistence
- [ ] Memory spilling works under pressure
- [ ] Temporal queries return correct results
- [ ] Performance within 2x of Rust DBSP implementation
- [ ] Comprehensive test coverage (>80%)

## Code Patterns to Follow

### Correct Type Constraints
```fsharp
type StorageBackend<'K, 'V when 'K : comparison and 'V : equality>() =
    // Implementation
```

### Proper Task Usage
```fsharp
member _.AsyncOperation() = 
    task {
        let! result = someAsyncOperation()
        return result
    }
```

### HashMap Operations
```fsharp
let mutable storage = HashMap.empty<'K, 'V * int64>
storage <- HashMap.add key (value, weight) storage
let result = HashMap.tryFind key storage
```

## References

- [DBSP Crate Storage](source_code_references/feldera/crates/dbsp/src/storage/)
- [ZoneTree Documentation](https://github.com/koculu/ZoneTree)
- [FSharp.Data.Adaptive](https://github.com/fsprojects/FSharp.Data.Adaptive)
- [MessagePack for F#](https://github.com/pocketberserker/MessagePack.FSharpExtensions)

## Notes

The current implementation provides a solid architectural foundation but requires completion of the core functionality. The main challenge is properly integrating ZoneTree with F# types while maintaining the performance characteristics required for incremental computation. The use of FSharp.Data.Adaptive's HashMap is correct for achieving O(1) operations as specified in the requirements.