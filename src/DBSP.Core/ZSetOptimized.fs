/// Optimized ZSet implementation based on Phase 5.1 benchmark findings
module DBSP.Core.ZSetOptimized

open System.Runtime.CompilerServices
open FSharp.Data.Adaptive
open DBSP.Core.ZSet

/// Memory-optimized ZSet operations to eliminate allocation hotspots
module ZSetOptimized =
    
    /// Builder pattern for efficient ZSet construction
    type ZSetBuilder<'K when 'K : comparison>() =
        let mutable inner = HashMap.empty<'K, int>
        
        member _.Add(key: 'K, weight: int) =
            if weight <> 0 then
                inner <- HashMap.alter key (function
                    | Some existing -> 
                        let newWeight = existing + weight
                        if newWeight = 0 then None else Some newWeight
                    | None -> Some weight
                ) inner
        
        member _.Build() = { ZSet.Inner = inner }
        
        member this.AddRange(pairs: seq<'K * int>) =
            for (key, weight) in pairs do
                this.Add(key, weight)
    
    /// Create ZSet using builder pattern for optimal performance
    let buildZSet (builderFn: ZSetBuilder<'K> -> unit) =
        let builder = ZSetBuilder<'K>()
        builderFn builder
        builder.Build()
    
    /// Optimized fromList that uses builder
    let ofListOptimized (pairs: ('K * int) list) =
        let builder = ZSetBuilder<'K>()
        builder.AddRange(pairs)
        builder.Build()

/// Performance analysis tools
module PerformanceAnalysis =
    
    /// Measure memory allocation of operations
    let measureAllocation (operation: unit -> 'T) =
        System.GC.Collect()
        System.GC.WaitForPendingFinalizers()
        System.GC.Collect()
        
        let memBefore = System.GC.GetTotalMemory(false)
        let result = operation()
        let memAfter = System.GC.GetTotalMemory(false)
        
        (result, memAfter - memBefore)