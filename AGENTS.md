# Repository Guidelines

This file provides guidance to automated AI agents when working with code in this repository.

## Repository Overview

This repository contains research and development materials for **DBSP.NET**, an F# implementation plan for the DBSP (Database Stream Processor) computational framework. DBSP enables **incremental computation** on changing datasets, where changes propagate in time proportional to change size rather than dataset size.

**Key Characteristics:**
- **Research Project**: Implementation planning and reference materials
- **Target Platform**: F#/.NET ecosystem
- **Core Technology**: DBSP incremental computation framework
- **Mathematical Foundation**: Algebraic structures (groups, rings, Z-sets)

## Project Structure

### Core Documents

- **`IMPLEMENTATION_PLAN.md`** - Comprehensive F# implementation roadmap with:
  - Mathematical foundations and algebraic type system
  - Core data structures (Z-sets, Indexed Z-sets, Streams)
  - Operator system architecture and circuit runtime
  - Module structure and development phases
  - Performance targets and optimization strategies

### Reference Materials (`source_code_references/`)

This directory contains reference implementations and documentation from existing DBSP implementations:

#### **`feldera/`** - Primary Reference Implementation (Rust)
- **Production DBSP Engine**: Complete Rust implementation with multi-language ecosystem
- **Architecture**: 14 Rust crates, Java SQL compiler, TypeScript web console, Python SDK
- **Key Components**:
  - `crates/dbsp/` - Core incremental computation engine
  - `sql-to-dbsp-compiler/` - Java-based SQL to DBSP compiler (Apache Calcite)
  - `crates/pipeline-manager/` - REST API server for pipeline management
  - `web-console/` - Svelte/TypeScript web UI
  - `python/` - Python SDK and client library

#### **`pydbsp/`** - Python Reference Implementation
- **Research Tool**: Pure Python implementation for experimentation
- **Zero Dependencies**: Educational implementation of DBSP concepts
- **Features**: Complete DBSP paper walkthrough implementation

#### **`dbsp-from-scratch/`** - Educational Implementation
- **Tutorial Focus**: Step-by-step DBSP implementation
- **Learning Resource**: Mirrors DBSP paper concepts accessibly

#### **Blog Posts**
Reference articles explaining core DBSP concepts:
- `Database computations on Z-sets.md` - Z-set mathematical foundations
- `Indexed Z-sets.md` - Efficient grouping and join implementations  
- `Implementing Z-sets.md` - Practical Z-set implementation patterns
- `Incremental Database Computations.md` - Overview of incremental computation

## Key Development Commands for F# Implementation

Based on the implementation plan, these would be the primary development commands:

### Build and Development

```fsharp
// Core module development structure
dotnet new sln -n DBSP.NET
dotnet new classlib -n DBSP.Core
dotnet new classlib -n DBSP.Operators
dotnet new classlib -n DBSP.Circuit
dotnet new classlib -n DBSP.Storage

// Testing
dotnet test
dotnet test --logger trx --collect:"XPlat Code Coverage"

// Package management
dotnet restore
dotnet build --configuration Release
```

### Module Architecture (Planned)

```
DBSP.NET/
├── DBSP.Core/
│   ├── Algebra.fs              // Algebraic foundations
│   ├── ZSet.fs                 // Z-set implementation
│   ├── IndexedZSet.fs          // Indexed collections
│   └── Stream.fs               // Stream abstraction
├── DBSP.Operators/
│   ├── Linear/Map.fs           // Stateless transformations
│   ├── Bilinear/Join.fs        // Join operations
│   └── Aggregation/GroupBy.fs  // Aggregation operators
└── DBSP.Circuit/
    ├── Builder.fs              // Circuit construction
    └── Runtime.fs              // Execution engine
```

## Implementation Priorities

### Phase 1: Core Foundations
1. Implement algebraic type system (Semigroup, Monoid, Group, Ring)
2. Create Z-set implementation with add/negate operations
3. Implement indexed Z-sets for efficient grouping
4. Build stream abstraction with basic operations

### Phase 2: Basic Operators  
1. Linear operators (Map, Filter, FlatMap)
2. Basic join operator (hash join)
3. Simple aggregation (SUM, COUNT, AVG)
4. Union and minus operators

### Phase 3: Circuit Runtime
1. Circuit builder API design
2. Single-threaded circuit execution
3. Input/output handles for data ingestion
4. Basic circuit optimization (operator fusion)

## Core Concepts and Architecture

### Mathematical Foundations

**Algebraic Structures**: The F# implementation should center on these mathematical abstractions:
- **Semigroup**: Associative binary operation
- **Monoid**: Semigroup with identity element  
- **Group**: Monoid with inverse operation
- **Ring**: Group with multiplication operation

**Z-Sets**: Collections with multiplicities supporting:
- Positive weights (insertions)
- Negative weights (deletions) 
- Addition and subtraction operations
- Mathematical correctness guarantees

### Stream Processing Model

**Incremental Computation**:
- Process only changes, not entire datasets
- Change propagation through operator circuits
- Bounded memory usage regardless of dataset size
- Mathematical guarantees of correctness

**Circuit Architecture**:
- SQL queries compile to circuits of interconnected operators
- Operators maintain incremental state
- Data flows as streams of changes (deltas)

## Technology Integration Points

### F#/.NET Ecosystem Integration

**Potential Integration Areas**:
- **Entity Framework Core**: Database connectivity
- **Reactive Extensions (Rx.NET)**: Stream processing integration
- **Apache Arrow**: Columnar data format
- **Orleans**: Distributed actor framework
- **ASP.NET Core**: API development

### Performance Considerations

**Optimization Targets** (from implementation plan):
- Sub-millisecond latency for simple operations  
- Linear scaling with data size for incremental updates
- Competitive performance with Rust reference implementation
- Multi-threading with F# async workflows

## Development Best Practices

### Code Organization
- **Functional-First**: Leverage F#'s functional programming strengths
- **Type Safety**: Use F#'s type system for mathematical correctness
- **Immutability**: Default to immutable data structures
- **Composition**: Build complex operators from simple primitives

### Testing Strategy
- **Property-Based Testing**: Use FsCheck for mathematical properties
- **Unit Testing**: Comprehensive coverage of algebraic operations
- **Integration Testing**: End-to-end circuit execution validation
- **Performance Testing**: Benchmark against reference implementations

## Reference Implementation Study

### Learning from Feldera (Rust Implementation)

**Architecture Patterns to Study**:
- Multi-crate organization with clear separation of concerns
- Visitor pattern for circuit transformations
- Type-safe operator composition
- Efficient data structure selection

**Key Files to Reference**:
- `crates/dbsp/src/algebra/` - Algebraic foundations
- `crates/dbsp/src/operator/` - Operator implementations  
- `crates/dbsp/src/circuit/` - Circuit execution engine
- `crates/dbsp/src/trace/` - Data structure implementations

### Learning from PyDBSP

**Implementation Insights**:
- Clean separation of mathematical concepts
- Educational implementation of complex algorithms
- Zero-dependency approach for core concepts

## Project Goals and Success Criteria

### Minimum Viable Product (MVP)
- [ ] Basic Z-set operations working correctly
- [ ] Simple circuit with Map/Filter/Aggregate
- [ ] Incremental computation correctness
- [ ] Performance within 10x of Python reference implementation

### Production Ready
- [ ] Full operator suite implemented
- [ ] Performance within 2x of Rust reference implementation  
- [ ] Comprehensive documentation and examples
- [ ] Integration with .NET ecosystem

This F# implementation aims to bring DBSP's powerful incremental computation capabilities to the .NET ecosystem while maintaining the mathematical rigor and performance characteristics of the reference implementations.
- Before declaring that a task is complete, if it may have had a performance impact, run the ./test-regression.sh script to ensure there are no performance regressions.  Ensure that performance regression tests are added for performance-sensitive code.
- Ensure that after completing a task, there are no compilation warnings or errors, and that all tests pass without any warnings or errors.