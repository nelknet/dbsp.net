#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Runs DBSP.NET benchmarks and performs regression analysis using BenchmarkDotNet.Analyser

.DESCRIPTION
    This script:
    1. Runs BenchmarkDotNet benchmarks with proper JSON output for BDNA
    2. Uses BDNA to analyze benchmark results for performance regressions
    3. Reports any significant performance degradations
    4. Stores results in git-friendly directory structure

.PARAMETER Filter
    Optional filter for specific benchmark classes (e.g., "*ZSet*", "*Operator*")

.PARAMETER Quick
    Run quick benchmarks for development (shorter iterations)

.PARAMETER Tolerance
    Regression tolerance percentage (default: 10%)

.EXAMPLE
    ./scripts/run-benchmark-analysis.ps1
    Run all benchmarks with regression analysis

.EXAMPLE
    ./scripts/run-benchmark-analysis.ps1 -Filter "*ZSet*" -Quick
    Run only ZSet benchmarks in quick mode
#>

param(
    [string]$Filter = "*",
    [switch]$Quick,
    [double]$Tolerance = 10.0
)

$ErrorActionPreference = "Stop"

# Ensure we're in the repository root
if (-not (Test-Path "DBSP.NET.sln")) {
    Write-Error "Must run from repository root (where DBSP.NET.sln is located)"
    exit 1
}

Write-Host "🔬 DBSP.NET Performance Benchmark Analysis" -ForegroundColor Cyan
Write-Host "=============================================" -ForegroundColor Cyan

# Get current commit information
$commitSha = (git rev-parse --short HEAD).Trim()
$branchName = (git branch --show-current).Trim()
$timestamp = Get-Date -Format "yyyy-MM-dd_HH-mm-ss"

Write-Host "📊 Benchmark Run Information:" -ForegroundColor Green
Write-Host "   Commit SHA: $commitSha"
Write-Host "   Branch: $branchName" 
Write-Host "   Timestamp: $timestamp"
Write-Host "   Filter: $Filter"
Write-Host "   Quick Mode: $Quick"
Write-Host ""

# Prepare benchmark results directory
$resultsDir = "benchmark_results"
if (-not (Test-Path $resultsDir)) {
    New-Item -ItemType Directory -Path $resultsDir | Out-Null
}

# Prepare BDNA analysis directory  
$bdnaDir = "benchmark_analysis"
if (-not (Test-Path $bdnaDir)) {
    New-Item -ItemType Directory -Path $bdnaDir | Out-Null
}

try {
    # Build the performance test project
    Write-Host "🔨 Building performance test project..." -ForegroundColor Yellow
    dotnet build test/DBSP.Tests.Performance/DBSP.Tests.Performance.fsproj -c Release -q
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Failed to build performance test project"
        exit 1
    }

    # Run benchmarks with appropriate arguments
    Write-Host "⚡ Running benchmarks..." -ForegroundColor Yellow
    $benchmarkArgs = @("--artifacts", $resultsDir)
    if ($Quick) {
        $benchmarkArgs += "--job", "short"
    }
    if ($Filter -ne "*") {
        $benchmarkArgs += "--filter", $Filter
    }

    Push-Location test/DBSP.Tests.Performance
    dotnet run -c Release -- $benchmarkArgs
    $benchmarkExitCode = $LASTEXITCODE
    Pop-Location

    if ($benchmarkExitCode -ne 0) {
        Write-Warning "Benchmarks completed with warnings or errors"
    }

    Write-Host "📈 Analyzing benchmark results with BDNA..." -ForegroundColor Yellow
    
    # Find the JSON report files
    $jsonFiles = Get-ChildItem -Path $resultsDir -Filter "*-report-full.json" -Recurse
    if ($jsonFiles.Count -eq 0) {
        Write-Warning "No JSON benchmark reports found. BDNA analysis skipped."
        Write-Host "💡 Available files in $resultsDir :" 
        Get-ChildItem -Path $resultsDir -Recurse | ForEach-Object { Write-Host "   $($_.FullName)" }
        exit 0
    }

    Write-Host "📋 Found $($jsonFiles.Count) benchmark report(s):"
    $jsonFiles | ForEach-Object { Write-Host "   $($_.Name)" }

    # Run BDNA analysis
    $bdnaArgs = @(
        "analyze",
        "--input", $resultsDir,
        "--output", $bdnaDir,
        "--tolerance", $Tolerance.ToString()
    )

    Write-Host "🔍 Running BDNA with tolerance: $Tolerance%" -ForegroundColor Green
    dotnet bdna @bdnaArgs
    $bdnaExitCode = $LASTEXITCODE

    # Report results
    Write-Host ""
    if ($bdnaExitCode -eq 0) {
        Write-Host "✅ No performance regressions detected!" -ForegroundColor Green
    } else {
        Write-Host "⚠️  Performance regression detected!" -ForegroundColor Red
        Write-Host "   Check the analysis output in $bdnaDir" -ForegroundColor Red
    }

    # Show analysis output if available
    $analysisReport = Get-ChildItem -Path $bdnaDir -Filter "*.json" -Recurse | Select-Object -First 1
    if ($analysisReport) {
        Write-Host ""
        Write-Host "📄 Analysis Report: $($analysisReport.FullName)" -ForegroundColor Cyan
        if (Test-Path $analysisReport.FullName) {
            $content = Get-Content $analysisReport.FullName -Raw | ConvertFrom-Json
            if ($content.degradations -and $content.degradations.Count -gt 0) {
                Write-Host "🔻 Performance Degradations Found:" -ForegroundColor Red
                $content.degradations | ForEach-Object {
                    Write-Host "   $($_.benchmark): $([math]::Round($_.degradationPercentage, 2))% slower" -ForegroundColor Red
                }
            }
        }
    }

    Write-Host ""
    Write-Host "📁 Results stored in: $resultsDir" -ForegroundColor Cyan
    Write-Host "📊 Analysis stored in: $bdnaDir" -ForegroundColor Cyan
    Write-Host ""

    exit $bdnaExitCode

} catch {
    Write-Host "❌ Error during benchmark analysis: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}