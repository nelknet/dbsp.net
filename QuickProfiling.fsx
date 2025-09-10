#!/usr/bin/env dotnet fsi

#r "nuget: FSharp.Data.Adaptive, 1.2.25"
#load "src/DBSP.Core/ZSet.fs"

open System
open System.Diagnostics
open DBSP.Core.ZSet
open FSharp.Data.Adaptive

// Quick profiling script to identify bottlenecks

let time label f =
    let sw = Stopwatch.StartNew()
    let result = f()
    sw.Stop()
    printfn "%s: %d ms" label sw.ElapsedMilliseconds
    result

// Test data
let dataSize = 10000
let changeSize = 1000

printfn "=== Performance Bottleneck Analysis ==="
printfn "Data size: %d, Change size: %d" dataSize changeSize

// Generate test data
let baseData = [| for i in 1..dataSize -> (i, $"value_{i}") |]
let changes = [| for i in 1..changeSize -> (Random.Shared.Next(1, dataSize+1), $"updated_{i}") |]

// Test 1: Naive recalculation
let naive() =
    let changeMap = changes |> Map.ofArray
    baseData 
    |> Array.map (fun (id, value) ->
        match Map.tryFind id changeMap with
        | Some newValue -> (id, newValue)
        | None -> (id, value))
    |> Array.length

// Test 2: DBSP with ZSets  
let dbspZSet() =
    // Create base ZSet
    let baseZSet = time "  ZSet.ofSeq base" (fun () ->
        ZSet.ofSeq (baseData |> Seq.map (fun kv -> (kv, 1))))
    
    // Create deletes
    let deletes = time "  Generate deletes" (fun () ->
        changes 
        |> Array.choose (fun (id, _) ->
            baseData |> Array.tryFind (fun (i, _) -> i = id)
            |> Option.map (fun oldKv -> (oldKv, -1))))
    
    // Create inserts
    let inserts = changes |> Array.map (fun kv -> (kv, 1))
    
    // Build delta
    let deltaZSet = time "  ZSet.ofSeq delta" (fun () ->
        ZSet.ofSeq (Seq.append deletes inserts))
    
    // Apply delta
    let result = time "  ZSet.add" (fun () ->
        ZSet.add baseZSet deltaZSet)
    
    // Count
    time "  Count non-zero" (fun () ->
        result.Inner 
        |> HashMap.filter (fun _ weight -> weight <> 0)
        |> HashMap.count)

// Test 3: Direct HashMap operations
let directHashMap() =
    let mutable map = time "  Create initial HashMap" (fun () ->
        let mutable m = HashMap.empty<int * string, int>
        for kv in baseData do
            m <- HashMap.add kv 1 m
        m)
    
    time "  Apply changes" (fun () ->
        for (id, newValue) in changes do
            // Remove old if exists
            match baseData |> Array.tryFind (fun (i, _) -> i = id) with
            | Some oldKv -> map <- HashMap.remove oldKv map
            | None -> ()
            // Add new
            map <- HashMap.add (id, newValue) 1 map
        HashMap.count map)

// Run tests
printfn "\n1. Naive Recalculation:"
let naiveResult = time "Total" naive

printfn "\n2. DBSP with ZSets:"
let dbspResult = time "Total" (fun () -> dbspZSet())

printfn "\n3. Direct HashMap Operations:"
let hashMapResult = time "Total" directHashMap

// Memory allocation test
printfn "\n=== Memory Allocation Patterns ==="

let testAllocation size =
    // Singleton pattern (anti-pattern)
    let singleton() =
        let mutable zset = ZSet.empty<int * string>
        for i in 1..size do
            let single = ZSet.singleton (i, $"v{i}") 1
            zset <- ZSet.add zset single
        zset
    
    // Batch pattern (optimized)
    let batch() =
        let pairs = [| for i in 1..size -> ((i, $"v{i}"), 1) |]
        ZSet.ofSeq pairs
    
    let gcBefore = GC.GetTotalMemory(true)
    let _ = time "  Singleton pattern" singleton
    let gcAfterSingleton = GC.GetTotalMemory(false)
    
    let gcBeforeBatch = GC.GetTotalMemory(true)
    let _ = time "  Batch pattern" batch
    let gcAfterBatch = GC.GetTotalMemory(false)
    
    printfn "  Singleton allocated: %d KB" ((gcAfterSingleton - gcBefore) / 1024L)
    printfn "  Batch allocated: %d KB" ((gcAfterBatch - gcBeforeBatch) / 1024L)

printfn "\nTesting with 1000 operations:"
testAllocation 1000

printfn "\n=== Analysis Summary ==="
let speedup = float naiveResult / float dbspResult
printfn "DBSP vs Naive speedup: %.2fx" speedup
if speedup < 1.0 then
    printfn "⚠️  DBSP is %.1fx SLOWER than naive!" (1.0 / speedup)
else
    printfn "✅ DBSP is %.1fx faster than naive!" speedup