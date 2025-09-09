#!/bin/bash
# DBSP.NET Performance Regression Testing
# Runs benchmarks and detects performance regressions using BDNA
# Usage: ./test-regression.sh [--quick] [--filter pattern] [--comprehensive]

set -e

# Configuration
FILTER="*"
QUICK=false
COMPREHENSIVE=false
TOLERANCE=10.0

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --quick)
            QUICK=true
            shift
            ;;
        --filter)
            FILTER="$2"
            shift 2
            ;;
        --comprehensive)
            COMPREHENSIVE=true
            shift
            ;;
        --tolerance)
            TOLERANCE="$2"
            shift 2
            ;;
        -h|--help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --quick            Run quick benchmarks for development"
            echo "  --filter PATTERN   Filter benchmarks (e.g., '*ZSet*')"
            echo "  --comprehensive    Run comprehensive Phase 5.2 benchmarks"
            echo "  --tolerance PERCENT Regression tolerance (default: 10.0%)"
            echo "  -h, --help         Show this help message"
            exit 0
            ;;
        *)
            echo "Unknown argument: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

echo "🔬 DBSP.NET Performance Regression Testing"
echo "=========================================="

# Run appropriate benchmark suite
if [[ "$COMPREHENSIVE" == "true" ]]; then
    echo "📊 Running Comprehensive Phase 5.2 Benchmarks"
    echo "   Git SHA: $(git rev-parse --short HEAD)"
    echo "   Branch: $(git branch --show-current)"
    echo ""
    
    # Run specific Phase 5.2 benchmarks
    ./scripts/run-benchmark-analysis.sh --filter "*ZSet_BatchConstruction*" --quick
    ./scripts/run-benchmark-analysis.sh --filter "*ThreadLocal*" --quick
    ./scripts/run-benchmark-analysis.sh --filter "*Parallel*" --quick
else
    # Run standard regression test
    if ! ./scripts/run-benchmark-analysis.sh ${QUICK:+--quick} ${FILTER:+--filter "$FILTER"} ${TOLERANCE:+--tolerance "$TOLERANCE"}; then
        echo "❌ Performance regression detected - see output above"
        exit 1
    fi
fi

echo "✅ No performance regression detected"
exit 0