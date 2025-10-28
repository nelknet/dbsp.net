# DBSP.NET

**A high-performance F# implementation of the Database Stream Processor (DBSP) computational framework for incremental view maintenance**

[![.NET](https://img.shields.io/badge/.NET-9.0-512BD4)](https://dotnet.microsoft.com/download/dotnet/9.0)
[![F#](https://img.shields.io/badge/F%23-8.0-378BBA)](https://fsharp.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Build Status](https://img.shields.io/badge/build-passing-brightgreen)](./test-regression.sh)
[![Status](https://img.shields.io/badge/status-alpha-orange)](https://github.com/Nelknet/dbsp.net)

> **⚠️ ALPHA QUALITY SOFTWARE - PRE-PRODUCTION**
>
> DBSP.NET is currently in **alpha stage** and under active development. While the core functionality is implemented and tested, this software is **not yet production-ready**. APIs may change, and there may be bugs or performance issues. Use in production environments at your own risk.
>
> **Current Status:**
> - ✅ Core algebraic framework and data structures complete
> - ✅ Comprehensive operator suite implemented
> - ✅ Single and multi-threaded execution working
> - ✅ Persistent storage backend functional
> - 🚧 Fault tolerance and checkpointing in progress
> - 📋 SQL frontend, distributed execution, and production features planned
>
> **Recommended Use Cases:**
> - Research and experimentation
> - Prototype development
> - Learning incremental computation concepts
> - Contributing to the project
>
> For production workloads, consider the mature [Feldera](https://github.com/feldera/feldera) Rust implementation.

## Table of Contents

- [Overview](#overview)
- [Key Features](#key-features)
- [Mathematical Foundation](#mathematical-foundation)
- [Architecture](#architecture)
- [Implementation Status](#implementation-status)
- [Getting Started](#getting-started)
- [Usage Examples](#usage-examples)
- [Performance](#performance)
- [Testing](#testing)
- [Documentation](#documentation)
- [Reference Materials](#reference-materials)
- [Contributing](#contributing)
- [License](#license)

## Overview

DBSP.NET brings the power of **incremental computation** to the .NET ecosystem through a functional-first F# implementation of the Database Stream Processor framework. Originally developed at VMware Research and implemented in Rust (Feldera), DBSP enables database computations to update results incrementally as data changes, achieving performance proportional to the size of changes rather than the entire dataset.

### What is DBSP?

DBSP (Database Stream Processor) is a computational framework that:
- **Incrementalizes** any query expressible in relational algebra
- **Maintains** query results as data streams change over time
- **Guarantees** mathematical correctness through algebraic foundations
- **Scales** linearly with change size, not dataset size

### Why DBSP.NET?

- **F# Type Safety**: Leverages F#'s powerful type system for mathematical correctness
- **Functional Paradigm**: Natural expression of algebraic operations and immutability
- **.NET Integration**: Seamless integration with Entity Framework, ASP.NET Core, and the broader .NET ecosystem
- **Performance**: Native .NET performance with zero-allocation paths and SIMD optimizations
- **Cross-Platform**: Runs on Windows, Linux, and macOS via .NET 9.0

## Mathematical Foundation

DBSP.NET implements a rigorous mathematical framework based on algebraic structures:

### Algebraic Type Hierarchy

```fsharp
// Core algebraic structures
type ISemigroup<'T> = 
    abstract Add: 'T -> 'T -> 'T  // Associative operation

type IMonoid<'T> =
    inherit ISemigroup<'T>
    abstract Zero: 'T              // Identity element

type IGroup<'T> =
    inherit IMonoid<'T>
    abstract Neg: 'T -> 'T         // Inverse operation

type IRing<'T> =
    inherit IGroup<'T>
    abstract Mul: 'T -> 'T -> 'T   // Multiplication
```

### Z-Sets: The Core Data Structure

Z-sets are multisets with integer weights, supporting both positive (insertion) and negative (deletion) weights:

```fsharp
// Z-set represents a collection with multiplicities
type ZSet<'K> = Map<'K, int64>

// Operations preserve algebraic properties
let add (z1: ZSet<'K>) (z2: ZSet<'K>) : ZSet<'K> =
    // Weights are added; zeros are eliminated
    Map.unionWith (+) z1 z2 |> Map.filter (fun _ w -> w <> 0L)

let negate (z: ZSet<'K>) : ZSet<'K> =
    // Negate all weights for deletion
    Map.map (fun _ w -> -w) z
```

### Incremental Computation

The differentiation operator (D) converts streams to change streams:

```
D[s](t) = s(t) - s(t-1)
```

Integration (I) accumulates changes over time:

```
I[Δs](t) = Σ(i=0 to t) Δs(i)
```

## Architecture

### Module Structure

```
DBSP.NET/
├── src/
│   ├── DBSP.Core/           # Core algebraic types and data structures
│   │   ├── Algebra.fs       # Algebraic foundations
│   │   ├── ZSet.fs          # Z-set implementation
│   │   ├── IndexedZSet.fs   # Indexed collections
│   │   └── Stream.fs        # Stream abstractions
│   │
│   ├── DBSP.Operators/      # Operator implementations
│   │   ├── LinearOperators.fs      # Map, Filter, FlatMap
│   │   ├── JoinOperators.fs        # Various join strategies
│   │   ├── AggregateOperators.fs   # Aggregation operations
│   │   └── TemporalOperators.fs    # Time-based operations
│   │
│   ├── DBSP.Circuit/        # Runtime and execution engine
│   │   ├── Builder.fs       # Circuit construction API
│   │   ├── Runtime.fs       # Single-threaded execution
│   │   ├── ParallelRuntime.fs  # Multi-threaded execution
│   │   └── Optimizer.fs     # Circuit optimization
│   │
│   ├── DBSP.Storage/        # Persistent storage backend
│   │   ├── LSMStorage.fs    # LSM tree implementation
│   │   ├── TemporalStorage.fs  # Time-indexed storage
│   │   ├── Serialization.fs    # Pluggable serializers
│   │   └── Spilling.fs      # Memory management
│   │
│   └── DBSP.Diagnostics/    # Monitoring and debugging
│       ├── CircuitGraph.fs  # Visualization
│       ├── TraceMonitor.fs  # Performance tracking
│       └── StateValidation.fs  # Correctness checks
│
├── test/
│   ├── DBSP.Tests.Unit/     # Unit tests
│   ├── DBSP.Tests.Properties/  # Property-based tests
│   ├── DBSP.Tests.Performance/ # Benchmarks
│   └── DBSP.Tests.Storage/  # Storage-specific tests
│
└── benchmark_analysis/       # Performance analysis tools
```

### Circuit Architecture

DBSP computations are organized as directed graphs of operators:

```
Input → [Map] → [Filter] → [Join] → [Aggregate] → Output
          ↓                    ↑
        [Index] ←─────────────┘
```

Each operator maintains incremental state and processes only changes:

```fsharp
// Example circuit construction
let circuit = 
    CircuitBuilder()
        .AddInput<Person>("persons")
        .Map(fun p -> p.Name, p.Age)
        .Filter(fun (_, age) -> age >= 18)
        .GroupBy(fst, fun group -> Seq.length group)
        .Output("adult_count")
        .Build()
```

### Performance Metrics

Current performance characteristics (Phase 5.3):

- **Throughput**: 25K-1.4M updates/second depending on data size and structure
- **Latency**: Microsecond to sub-millisecond for incremental updates
- **Memory**: O(changes) space complexity
- **Storage**: 100K+ ops/sec write, 200K-500K ops/sec read
- **Scaling**: Near-linear with CPU cores (85-95% efficiency)

## Getting Started

### Prerequisites

- [.NET 9.0 SDK](https://dotnet.microsoft.com/download/dotnet/9.0) or later
- F# 8.0 or later
- Optional: Visual Studio 2022, VS Code, or JetBrains Rider

### Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/dbsp.net.git
cd dbsp.net

# Restore dependencies
dotnet restore

# Build the solution
dotnet build --configuration Release

# Run tests
dotnet test
```

### Quick Start

```fsharp
#r "nuget: DBSP.Core"
#r "nuget: DBSP.Operators"
#r "nuget: DBSP.Circuit"

open DBSP.Core
open DBSP.Operators
open DBSP.Circuit

// Define data types
type Order = { OrderId: int; CustomerId: int; Amount: decimal }
type Customer = { CustomerId: int; Name: string; Country: string }

// Build an incremental query circuit
let circuit = 
    CircuitBuilder()
        // Input streams
        .AddInput<Order>("orders")
        .AddInput<Customer>("customers")
        
        // Join orders with customers
        .Join(
            "orders", 
            "customers",
            fun o -> o.CustomerId,
            fun c -> c.CustomerId,
            fun o c -> { OrderId = o.OrderId; CustomerName = c.Name; Amount = o.Amount; Country = c.Country }
        )
        
        // Filter to specific country
        .Filter(fun joined -> joined.Country = "USA")
        
        // Aggregate by customer
        .GroupBy(
            fun j -> j.CustomerName,
            fun group -> group |> Seq.sumBy (fun j -> j.Amount)
        )
        
        // Output results
        .Output("revenue_by_customer")
        .Build()

// Process incremental updates
let runtime = CircuitRuntime(circuit)

// Insert initial data
runtime.SendInput("orders", 
    zset [
        { OrderId = 1; CustomerId = 101; Amount = 100m }, 1L
        { OrderId = 2; CustomerId = 102; Amount = 200m }, 1L
    ])

runtime.SendInput("customers",
    zset [
        { CustomerId = 101; Name = "Alice"; Country = "USA" }, 1L
        { CustomerId = 102; Name = "Bob"; Country = "USA" }, 1L
    ])

runtime.Step() // Process changes

// Get incremental results
let results = runtime.GetOutput("revenue_by_customer")
// Results: [("Alice", 100m); ("Bob", 200m)]

// Update order amount (delete old, insert new)
runtime.SendInput("orders",
    zset [
        { OrderId = 1; CustomerId = 101; Amount = 100m }, -1L  // Delete
        { OrderId = 1; CustomerId = 101; Amount = 150m }, 1L   // Insert
    ])

runtime.Step()
let deltaResults = runtime.GetOutput("revenue_by_customer")
// Delta: [("Alice", 50m)]  // Only the change!
```

## Usage Examples

### Example 1: Real-time Analytics Dashboard

```fsharp
// Track website events incrementally
type Event = { 
    UserId: string
    EventType: string  
    Timestamp: DateTime
    Value: float option 
}

let analyticsCircuit =
    CircuitBuilder()
        .AddInput<Event>("events")
        
        // Count events by type
        .GroupBy(
            fun e -> e.EventType,
            fun group -> int64 (Seq.length group)
        )
        .Output("events_by_type")
        
        // Track unique users
        .Map(fun e -> e.UserId)
        .Distinct()
        .Count()
        .Output("unique_users")
        
        // Calculate average value where present
        .Filter(fun e -> e.Value.IsSome)
        .GroupBy(
            fun e -> e.EventType,
            fun group -> 
                let values = group |> Seq.choose (fun e -> e.Value)
                Seq.average values
        )
        .Output("avg_value_by_type")
        
        .Build()
```

### Example 2: Inventory Management

```fsharp
// Real-time inventory tracking with alerts
type StockMovement = {
    ProductId: int
    WarehouseId: int  
    Quantity: int  // Positive = inbound, Negative = outbound
    Timestamp: DateTime
}

type ProductInfo = {
    ProductId: int
    Name: string
    ReorderLevel: int
    MaxStock: int
}

let inventoryCircuit =
    CircuitBuilder()
        .AddInput<StockMovement>("movements")
        .AddInput<ProductInfo>("products")
        
        // Current stock levels by warehouse
        .GroupBy(
            "movements",
            fun m -> (m.ProductId, m.WarehouseId),
            fun group -> group |> Seq.sumBy (fun m -> int64 m.Quantity)
        )
        .Output("stock_levels")
        
        // Low stock alerts
        .Join(
            "stock_levels",
            "products",
            fun (productId, _) quantity -> productId,
            fun p -> p.ProductId,
            fun (productId, warehouseId) quantity product ->
                if quantity < int64 product.ReorderLevel then
                    Some { 
                        ProductId = productId
                        WarehouseId = warehouseId
                        ProductName = product.Name
                        CurrentStock = quantity
                        ReorderLevel = product.ReorderLevel 
                    }
                else None
        )
        .Filter(Option.isSome)
        .Map(Option.get)
        .Output("low_stock_alerts")
        
        .Build()
```

### Example 3: Financial Portfolio Tracking

```fsharp
// Incremental portfolio valuation
type Position = {
    AccountId: string
    Symbol: string
    Shares: decimal
}

type Price = {
    Symbol: string
    Price: decimal
    Timestamp: DateTime
}

let portfolioCircuit =
    CircuitBuilder()
        .AddInput<Position>("positions")
        .AddInput<Price>("prices")
        
        // Calculate position values
        .Join(
            "positions",
            "prices",
            fun pos -> pos.Symbol,
            fun price -> price.Symbol,
            fun pos price -> {|
                AccountId = pos.AccountId
                Symbol = pos.Symbol
                Shares = pos.Shares
                Price = price.Price
                Value = pos.Shares * price.Price
            |}
        )
        
        // Portfolio value by account
        .GroupBy(
            fun pv -> pv.AccountId,
            fun group -> group |> Seq.sumBy (fun pv -> pv.Value)
        )
        .Output("portfolio_values")
        
        // Top positions by value
        .SortBy(fun pv -> -pv.Value)
        .Take(10)
        .Output("top_positions")
        
        .Build()
```

## Performance

### Benchmarking

DBSP.NET includes comprehensive benchmarks using BenchmarkDotNet:

```bash
# Run all benchmarks
dotnet run -c Release --project test/DBSP.Tests.Performance

# Run specific benchmark
dotnet run -c Release --project test/DBSP.Tests.Performance -- --filter "*ZSet*"

# Quick development benchmarks
dotnet run -c Release --project test/DBSP.Tests.Performance -- --job short
```

### Performance Characteristics

*Note: Performance measured on Apple M4 Max. Actual performance varies with data size and hardware.*

| Operation | Throughput | Latency | Memory |
|-----------|------------|---------|---------|
| **ZSet Addition (100 items)** | 350K ops/sec | ~3μs | O(distinct keys) |
| **ZSet Addition (1K items)** | 25K ops/sec | ~40μs | O(distinct keys) |
| **ZSet Addition (10K items)** | 2K ops/sec | ~450μs | O(distinct keys) |
| **FastZSet Addition (100 items)** | 1.4M ops/sec | ~700ns | O(distinct keys) |
| **FastZSet Addition (1K items)** | 140K ops/sec | ~7μs | O(distinct keys) |
| **Hash Join*** | 100K-1M ops/sec | 1-10μs | O(smaller relation) |
| **Aggregation*** | 200K-1M ops/sec | 1-5μs | O(groups) |
| **Filter*** | 1M-5M ops/sec | 200-1000ns | O(1) |
| **Map*** | 1M-3M ops/sec | 300-1000ns | O(1) |
| **Storage Write** | 100K ops/sec | ~10μs | Adaptive |
| **Storage Read** | 200K-500K ops/sec | ~2μs | Cached |

*Estimated based on data structure performance. Actual throughput depends on data characteristics and specific operations.

### Optimization Techniques

1. **Zero-Allocation Paths**: Using `Span<'T>` and stack allocation
2. **Operator Fusion**: Combining adjacent operators to reduce overhead
3. **Batch Processing**: Amortizing fixed costs across multiple updates
4. **Thread-Local Storage**: Eliminating synchronization in parallel execution
5. **Adaptive Algorithms**: Choosing optimal strategies based on data characteristics

## Testing

### Test Suites

```bash
# Unit tests (fast, isolated)
dotnet test test/DBSP.Tests.Unit

# Property-based tests (thorough, randomized)
dotnet test test/DBSP.Tests.Properties

# Storage tests (integration)
dotnet test test/DBSP.Tests.Storage

# Performance regression tests
./test-regression.sh

# Comprehensive test coverage
dotnet test --collect:"XPlat Code Coverage"
```

### Property-Based Testing

DBSP.NET uses FsCheck for property-based testing:

```fsharp
// Example: Z-set addition is commutative
[<Property>]
let ``ZSet addition is commutative`` (z1: ZSet<int>) (z2: ZSet<int>) =
    ZSet.add z1 z2 = ZSet.add z2 z1

// Example: Join preserves algebraic properties
[<Property>]
let ``Join distributes over addition`` (r1: ZSet<_>) (r2: ZSet<_>) (s: ZSet<_>) =
    join (add r1 r2) s = add (join r1 s) (join r2 s)
```

### Continuous Integration

The project includes GitHub Actions workflows for:
- Build and test on every commit
- Performance regression detection
- Code coverage reporting
- Cross-platform testing (Windows, Linux, macOS)

## Documentation

### API Documentation

Full API documentation is available at [docs/api/](docs/api/index.html) (generated using FSharp.Formatting).

### Tutorials

1. [Getting Started with DBSP.NET](docs/tutorials/getting-started.md)
2. [Building Your First Circuit](docs/tutorials/first-circuit.md)
3. [Understanding Z-Sets](docs/tutorials/understanding-zsets.md)
4. [Incremental Joins](docs/tutorials/incremental-joins.md)
5. [Performance Optimization](docs/tutorials/performance.md)

### Design Documents

- [Architecture Overview](docs/design/architecture.md)
- [Storage Design](src/DBSP.Storage/STORAGE_DESIGN.md)
- [Parallel Execution](docs/design/parallel-execution.md)
- [Mathematical Foundations](docs/design/mathematical-foundations.md)

## Reference Materials

This implementation is based on extensive research and existing implementations:

### Academic Papers

- [DBSP: Automatic Incremental View Maintenance for Rich Query Languages](https://arxiv.org/abs/2203.16684)
- [Differential Dataflow](https://github.com/TimelyDataflow/differential-dataflow/blob/master/differentialdataflow.pdf)

### Reference Implementations

- **[Feldera](https://github.com/feldera/feldera)**: Production Rust implementation
- **[PyDBSP](source_code_references/pydbsp/)**: Educational Python implementation
- **[DBSP From Scratch](source_code_references/dbsp-from-scratch/)**: Tutorial implementation

### Blog Posts

Located in `source_code_references/`:
- [Database Computations on Z-sets](source_code_references/Database%20computations%20on%20Z-sets.md)
- [Implementing Z-sets](source_code_references/Implementing%20Z-sets.md)
- [Indexed Z-sets](source_code_references/Indexed%20Z-sets.md)
- [Incremental Database Computations](source_code_references/Incremental%20Database%20Computations%20-%20Part%201.md)

## Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

### Development Setup

```bash
# Install development tools
dotnet tool restore

# Format code
dotnet fantomas src/ --recurse

# Run linting
dotnet fsharplint lint DBSP.NET.sln

# Generate documentation
dotnet fsdocs build --clean
```

### Code Style

- Follow F# coding conventions
- Use meaningful names
- Add XML documentation to public APIs
- Write tests for new features
- Ensure benchmarks for performance-critical code

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Examples

An end-to-end tutorial project lives under `examples/DBSP.Examples`. It demonstrates a realistic business scenario (orders per customer) and contrasts a naive recomputation approach against an incremental (DBSP-style) approach using ZSets and an Integrate operator.

Run the tutorial (defaults tuned for a clear contrast):

```
dotnet run --project examples/DBSP.Examples/DBSP.Examples.fsproj -c Release
```

What it does:
- Generates synthetic Customers and Orders.
- Naive: scans all orders every step to recompute counts per customer.
- Incremental: maintains counts by applying small ZSet deltas per step.
- Prints intermediate snapshots and totals for both, with a final timing summary.

Default parameters (override via CLI flags `--customers N --initial N --steps N --changes N`):
- customers = 100
- initial orders = 5,000,000
- steps = 60
- changes per step = 100

Example performance (modern ARM64 laptop, .NET 9 Release):
- Naive total: ~2.7 seconds
- Incremental total: ~9 milliseconds
- Speedup: ~300x


## Acknowledgments

- VMware Research for the original DBSP research
- The Feldera team for the production Rust implementation
- The F# community for excellent functional programming tools
- Contributors and early adopters

---

<div align="center">
Built with ❤️ using F# and .NET
</div>
```
