#!/bin/bash
# Simple wrapper for testing DBSP.NET performance regression detection
# Usage: ./test-regression.sh [--quick] [--filter pattern]

# Default to quick benchmarks for testing
QUICK_ARG=""
FILTER_ARG=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --quick)
            QUICK_ARG="--quick"
            shift
            ;;
        --filter)
            FILTER_ARG="--filter $2"
            shift 2
            ;;
        *)
            echo "Unknown argument: $1"
            echo "Usage: ./test-regression.sh [--quick] [--filter pattern]"
            exit 1
            ;;
    esac
done

echo "🔬 Testing DBSP.NET Performance Regression Detection"
echo "=================================================="

if ! ./scripts/run-benchmark-analysis.sh $QUICK_ARG $FILTER_ARG; then
    echo "❌ Performance regression detected - see output above"
    exit 1
else
    echo "✅ No performance regression detected"
    exit 0
fi