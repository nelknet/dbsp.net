# DBSP.NET Performance Benchmarking Guide

This document describes how to run performance benchmarks and regression analysis for DBSP.NET using BenchmarkDotNet and BenchmarkDotNet.Analyser (BDNA).

## Quick Start

### Run All Benchmarks with Regression Analysis
```bash
# Simple regression test (recommended for development)
./test-regression.sh --quick

# Cross-platform PowerShell (full benchmarks)
pwsh scripts/run-benchmark-analysis.ps1

# Unix/Linux/macOS (full benchmarks)
./scripts/run-benchmark-analysis.sh
```

### Run Specific Benchmarks
```bash
# Only ZSet benchmarks  
./test-regression.sh --filter "*ZSet*"

# Quick development benchmarks
./test-regression.sh --quick

# Custom regression tolerance (via full script)
./scripts/run-benchmark-analysis.sh --tolerance 5.0
```

## Manual Benchmark Execution

### Run Benchmarks Only (without regression analysis)
```bash
cd test/DBSP.Tests.Performance
dotnet run -c Release
```

### Run with Custom Filters
```bash
cd test/DBSP.Tests.Performance
dotnet run -c Release -- --filter "*HashMap*" --job short
```

## Regression Analysis

### Analyze Existing Results
```bash
# Analyze all benchmark results in benchmark_results/
dotnet bdna analyze --input benchmark_results --output benchmark_analysis

# With custom tolerance (5% regression threshold)
dotnet bdna analyze --input benchmark_results --output benchmark_analysis --tolerance 5.0
```

### Check Analysis Results
- **Exit Code 0**: No regressions detected
- **Exit Code 1**: Performance regressions found
- **Analysis Output**: Check `benchmark_analysis/` directory for detailed reports

## Benchmark Categories

### Core Data Structures (`DataStructureBenchmarks.fs`)
- **DataStructureComparisonBenchmarks**: HashMap vs F# Map vs Dictionary performance
- **ZSetOperationBenchmarks**: ZSet algebraic operations (add, negate, union)

### Operator Performance (`OperatorBenchmarks.fs`)
- **LinearOperatorBenchmarks**: Map, Filter, Negate operations
- **BinaryOperatorBenchmarks**: Union, Minus operations  
- **AggregationOperatorBenchmarks**: Count, Sum operations
- **JoinOperatorBenchmarks**: InnerJoin, SemiJoin operations
- **AsyncOverheadBenchmarks**: Direct vs operator async execution comparison

## Performance Tracking Workflow

### Development Workflow
1. **Make code changes**
2. **Run regression test**: `./scripts/run-benchmark-analysis.sh --quick`
3. **Check for regressions** (script exits with error if detected)
4. **Investigate regressions** in `benchmark_analysis/` output
5. **Commit only if no significant regressions**

### Release Workflow  
1. **Run full benchmark suite**: `./scripts/run-benchmark-analysis.sh`
2. **Document performance characteristics** for release notes
3. **Commit benchmark results** to preserve performance history

## Data Storage and History

### File Structure
```
benchmark_results/
├── 2025-01-15_10-30-00_abc123/
│   ├── DataStructureBenchmarks-report-full.json
│   ├── OperatorBenchmarks-report-full.json
│   └── results-*.csv
└── 2025-01-16_14-20-15_def456/
    └── ...

benchmark_analysis/
├── aggregated-results.json
├── analysis-report.json  
└── degradations.json
```

### Git Integration
- **Benchmark results** are stored as JSON files (human-readable in Git diffs)
- **Complete history** preserved in Git repository  
- **BDNA analyzes** the most recent 100 runs for active regression detection
- **Historical analysis** possible by checking out older commits

### Long-term Analysis
```bash
# Analyze performance at specific commit
git checkout <commit-sha>
dotnet bdna analyze --input benchmark_results --output analysis_historical

# Compare performance between milestones
git checkout phase-1-complete
./scripts/run-benchmark-analysis.sh
cp benchmark_analysis/analysis-report.json phase1_report.json

git checkout phase-2-complete  
./scripts/run-benchmark-analysis.sh
# Compare phase1_report.json vs current analysis results
```

## Troubleshooting

### No JSON Reports Generated
Ensure benchmark classes have required attributes:
```fsharp
[<MemoryDiagnoser>]
[<MinColumn>]
[<MaxColumn>] 
[<Q1Column>]
[<Q3Column>]
[<AllStatisticsColumn>]
type MyBenchmarks() = ...
```

### BDNA Analysis Fails
- Check that JSON reports contain full statistics
- Verify benchmark_results directory contains valid JSON files
- Ensure .NET 8 runtime is available for BDNA

### High Variance in Results
- Run benchmarks multiple times for consistency
- Use `--job short` for quick development testing
- Consider machine-specific performance characteristics

## Performance Targets

| Operation | Target Latency | Target Throughput |
|-----------|---------------|-------------------|
| Map | < 1μs per record | > 10M records/sec |
| Filter | < 1μs per record | > 10M records/sec |
| Join (indexed) | < 10μs per record | > 1M records/sec |
| Aggregation | < 5μs per record | > 2M records/sec |
| ZSet Union | < 100μs per 10K records | > 100K unions/sec |

## Integration with Development

### Pre-commit Hook (Optional)
```bash
#!/bin/bash
# .git/hooks/pre-commit
echo "Running performance regression test..."
if ! ./scripts/run-benchmark-analysis.sh --quick --tolerance 15.0; then
    echo "⚠️  Performance regression detected. Continue? (y/N)"
    read -r response
    if [[ ! "$response" =~ ^[Yy]$ ]]; then
        echo "Commit cancelled due to performance regression"
        exit 1
    fi
fi
```

### IDE Integration
Most IDEs can run the scripts directly:
- **VS Code**: Use terminal to run `./scripts/run-benchmark-analysis.sh`
- **Rider**: Run scripts from integrated terminal
- **Command line**: Direct execution from repository root