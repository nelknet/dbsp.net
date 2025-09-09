#!/bin/bash
# Phase 5.1: Data Structure Performance Optimization Benchmark Runner
# Comprehensive performance analysis for HashMap vs FastDict implementations

set -euo pipefail

echo "🚀 Phase 5.1: Data Structure Performance Optimization"
echo "=================================================="

# Get current git info
GIT_SHA=$(git rev-parse --short HEAD)
GIT_BRANCH=$(git branch --show-current)
TIMESTAMP=$(date '+%Y-%m-%d_%H-%M-%S')

echo "📊 Benchmark Information:"
echo "   Git SHA: $GIT_SHA"
echo "   Branch: $GIT_BRANCH" 
echo "   Timestamp: $TIMESTAMP"

# Create results directory
RESULTS_DIR="benchmark_results/phase5-1_${TIMESTAMP}_${GIT_SHA}"
mkdir -p "$RESULTS_DIR"

echo ""
echo "🔨 Building performance test project..."
dotnet build test/DBSP.Tests.Performance/ -c Release

echo ""
echo "📈 Running Phase 5.1 Data Structure Benchmarks..."
echo "   Results will be saved to: $RESULTS_DIR"

# Run specific Phase 5.1 benchmarks
cd test/DBSP.Tests.Performance/

echo ""
echo "1️⃣  Core Data Structure Comparison"
echo "   Benchmarking HashMap vs FastDict vs F# Map vs ImmutableDictionary..."
dotnet run -c Release -- --filter "*DataStructureCore*" --artifacts "$RESULTS_DIR/core"

echo ""  
echo "2️⃣  SRTP Performance Validation"
echo "   Measuring compile-time specialization vs interface dispatch..."
dotnet run -c Release -- --filter "*AlgebraicDispatch*" --artifacts "$RESULTS_DIR/srtp"

echo ""
echo "3️⃣  Memory Allocation Analysis"  
echo "   Profiling allocation patterns in ZSet operations..."
dotnet run -c Release -- --filter "*MemoryAllocation*" --artifacts "$RESULTS_DIR/memory"

echo ""
echo "4️⃣  Cache Locality Testing"
echo "   Testing sequential vs random access patterns..."
dotnet run -c Release -- --filter "*CacheLocality*" --artifacts "$RESULTS_DIR/cache"

echo ""
echo "✅ Phase 5.1 Benchmarks Complete!"
echo "📁 Results stored in: $RESULTS_DIR"
echo ""
echo "📊 Summary:"
echo "   - Core data structure performance comparison"
echo "   - SRTP vs interface dispatch overhead analysis"  
echo "   - Memory allocation pattern optimization"
echo "   - Cache locality and branch prediction analysis"
echo ""
echo "🔍 Next Steps:"
echo "   1. Analyze BenchmarkDotNet results in $RESULTS_DIR"
echo "   2. Identify performance bottlenecks"
echo "   3. Implement fastest data structure variant"
echo "   4. Run regression tests with ./test-regression.sh"