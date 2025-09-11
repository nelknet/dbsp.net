#!/bin/bash
set -e

# DBSP.NET Performance Benchmark Analysis Script
# Runs BenchmarkDotNet benchmarks and performs regression analysis using BDNA

# Default values
FILTER="*"
QUICK=false
TOLERANCE=10.0

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --filter)
            FILTER="$2"
            shift 2
            ;;
        --quick)
            QUICK=true
            shift
            ;;
        --tolerance)
            TOLERANCE="$2"
            shift 2
            ;;
        -h|--help)
            echo "Usage: $0 [--filter PATTERN] [--quick] [--tolerance PERCENT]"
            echo ""
            echo "Options:"
            echo "  --filter PATTERN    Filter benchmarks (e.g., '*ZSet*')"
            echo "  --quick            Run quick benchmarks for development"
            echo "  --tolerance PERCENT Regression tolerance (default: 10.0%)"
            echo "  -h, --help         Show this help message"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Ensure we're in repository root
if [[ ! -f "DBSP.NET.sln" ]]; then
    echo "❌ Error: Must run from repository root (where DBSP.NET.sln is located)"
    exit 1
fi

echo "🔬 DBSP.NET Performance Benchmark Analysis"
echo "============================================="

# Get current commit information  
COMMIT_SHA=$(git rev-parse --short HEAD)
BRANCH_NAME=$(git branch --show-current)
TIMESTAMP=$(date +"%Y-%m-%d_%H-%M-%S")

echo "📊 Benchmark Run Information:"
echo "   Commit SHA: $COMMIT_SHA"
echo "   Branch: $BRANCH_NAME"
echo "   Timestamp: $TIMESTAMP" 
echo "   Filter: $FILTER"
echo "   Quick Mode: $QUICK"
echo ""

# Prepare directories
RESULTS_DIR="benchmark_results"
BDNA_DIR="benchmark_analysis"

mkdir -p "$RESULTS_DIR"
mkdir -p "$BDNA_DIR"

# Build performance test project
echo "🔨 Building performance test project..."
if ! dotnet build test/DBSP.Tests.Performance/DBSP.Tests.Performance.fsproj -c Release --verbosity quiet; then
    echo "❌ Failed to build performance test project"
    exit 1
fi

# Run benchmarks
echo "⚡ Running benchmarks..."
BENCHMARK_ARGS=("--artifacts" "$RESULTS_DIR")

if [[ "$QUICK" == true ]]; then
    BENCHMARK_ARGS+=("--job" "short")
fi

# Always pass a filter to avoid interactive prompts (use '*' for all)
BENCHMARK_ARGS+=("--filter" "$FILTER")

cd test/DBSP.Tests.Performance
if ! dotnet run -c Release -- "${BENCHMARK_ARGS[@]}"; then
    echo "⚠️  Benchmarks completed with warnings or errors"
fi
cd ../..

echo "📈 Analyzing benchmark results with BDNA..."

# Ensure dotnet local tools are available (bdna)
dotnet tool restore >/dev/null 2>&1 || true

# Find JSON report files
JSON_FILES=$(find "$RESULTS_DIR" -name "*-report-full.json" -type f | wc -l)
if [[ $JSON_FILES -eq 0 ]]; then
    echo "⚠️  No JSON benchmark reports found. BDNA analysis skipped."
    echo "💡 Available files in $RESULTS_DIR:"
    find "$RESULTS_DIR" -type f | head -10
    exit 0
fi

echo "📋 Found $JSON_FILES benchmark report(s)"

# Aggregate benchmark results for BDNA
echo "📊 Aggregating benchmark results..."
AGGREGATED_DIR="$BDNA_DIR/aggregated"
EMPTY_DIR="$BDNA_DIR/empty"
mkdir -p "$AGGREGATED_DIR"
mkdir -p "$EMPTY_DIR"

# Find the latest results directory
LATEST_RESULTS=$(find "$RESULTS_DIR" -maxdepth 1 -type d -name "*_*" | sort | tail -1)
if [[ -z "$LATEST_RESULTS" ]]; then
    echo "❌ No timestamped results directory found"
    exit 1
fi

echo "📁 Latest results in: $LATEST_RESULTS"

# Check if this is the first run (empty aggregated directory)
if [[ $(find "$AGGREGATED_DIR" -name "*.json" -type f 2>/dev/null | wc -l) -eq 0 ]]; then
    echo "🆕 First benchmark run - creating initial dataset"
    AGGREGATES_SOURCE="$EMPTY_DIR"
else
    echo "🔄 Adding to existing benchmark dataset"  
    AGGREGATES_SOURCE="$AGGREGATED_DIR"
fi

# Aggregate results
if ! dotnet bdna aggregate \
    --new "$LATEST_RESULTS" \
    --aggregates "$AGGREGATES_SOURCE" \
    --output "$AGGREGATED_DIR" \
    --commit "$COMMIT_SHA" \
    --branch "$BRANCH_NAME" \
    --runs 100; then
    echo "❌ Failed to aggregate benchmark results"
    exit 1
fi

# Analyze for regressions
echo "🔍 Running BDNA regression analysis with tolerance: ${TOLERANCE}%"
if dotnet bdna analyse \
    --aggregates "$AGGREGATED_DIR" \
    --tolerance "$TOLERANCE" \
    --verbose; then
    echo "✅ No performance regressions detected!"
    BDNA_EXIT=0
else
    echo "⚠️  Performance regression detected!"
    echo "   Check the analysis output in $BDNA_DIR"
    BDNA_EXIT=1
fi

# Show results summary
echo ""
echo "📁 Results stored in: $RESULTS_DIR"
echo "📊 Analysis stored in: $BDNA_DIR"
echo ""

exit $BDNA_EXIT
