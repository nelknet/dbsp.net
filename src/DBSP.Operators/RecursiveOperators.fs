/// Recursive operators for DBSP circuits: fixed-point iteration
/// Enables recursive queries and iterative computations 
module DBSP.Operators.RecursiveOperators

open System.Runtime.CompilerServices
open System.Threading.Tasks
open DBSP.Core.ZSet
open DBSP.Operators.Interfaces

/// Fixed-point iteration operator for recursive computations
/// Iterates a function until it reaches a stable state (fixed point)
type FixedPointOperator<'T when 'T: comparison and 'T: equality>
    ([<InlineIfLambda>] iterativeFunc: ZSet<'T> -> Task<ZSet<'T>>, 
     ?maxIterations: int,
     ?tolerance: float,
     ?name: string) =
    inherit BaseUnaryOperator<ZSet<'T>, ZSet<'T>>(defaultArg name "FixedPoint")
    
    let maxIter = defaultArg maxIterations 1000
    let tol = defaultArg tolerance 1e-10
    let mutable currentIteration = 0
    let mutable converged = false
    let mutable lastResult: ZSet<'T> option = None
    
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    override _.EvalAsyncImpl(input: ZSet<'T>) = task {
        currentIteration <- 0
        converged <- false
        let mutable current = input
        let mutable previous = ZSet.empty<'T>
        
        // Iterate until convergence or max iterations
        while not converged && currentIteration < maxIter do
            currentIteration <- currentIteration + 1
            
            // Apply the iterative function
            let! next = iterativeFunc current
            
            // Check for convergence (no change)
            if next = current then
                converged <- true
            else
                previous <- current
                current <- next
                
                // Check if we're oscillating or converging slowly
                if currentIteration % 10 = 0 then
                    let changeSize = ZSet.difference next current |> (fun zs -> zs.Count)
                    if float changeSize < tol then
                        converged <- true
        
        lastResult <- Some current
        return current
    }
    
    /// Get the number of iterations performed in the last computation
    member _.LastIterationCount = currentIteration
    
    /// Whether the last computation converged
    member _.HasConverged = converged
    
    /// Get the last result
    member _.LastResult = lastResult
    
    /// Reset the operator state
    member _.Reset() =
        currentIteration <- 0
        converged <- false
        lastResult <- None
    
    interface IUnaryOperator<ZSet<'T>, ZSet<'T>> with
        member this.EvalAsync(input) = this.EvalAsyncImpl(input)
        member _.InputPreference = NoPreference

/// Recursive query builder for common recursion patterns
type RecursiveQueryBuilder<'T when 'T: comparison and 'T: equality>() =
    
    /// Build transitive closure computation
    member _.TransitiveClosure(edges: ZSet<'T * 'T>) = 
        let transitiveFunc = fun (paths: ZSet<'T * 'T>) -> task {
            // Join paths with edges to extend paths
            let newPaths = 
                ZSet.toSeq paths
                |> Seq.collect (fun ((from, via), pathWeight) ->
                    ZSet.toSeq edges
                    |> Seq.choose (fun ((start, target), edgeWeight) ->
                        if via = start then
                            Some ((from, target), pathWeight * edgeWeight)
                        else None))
                |> ZSet.ofSeq
            
            // Union with existing paths
            return ZSet.union paths newPaths
        }
        
        FixedPointOperator<'T * 'T>(transitiveFunc, maxIterations = 100)
    
    /// Build connected components computation 
    member _.ConnectedComponents(edges: ZSet<'T * 'T>) =
        let componentFunc = fun (components: ZSet<'T * 'T>) -> task {
            // Propagate minimum component ID to all reachable nodes
            let propagated =
                ZSet.toSeq components
                |> Seq.collect (fun ((node, comp), weight) ->
                    ZSet.toSeq edges
                    |> Seq.choose (fun ((from, target), edgeWeight) ->
                        if from = node then
                            Some ((target, comp), weight * edgeWeight)
                        else None))
                |> ZSet.ofSeq
            
            // Take minimum component for each node
            let minComponents = 
                ZSet.union components propagated
                |> ZSet.toSeq
                |> Seq.groupBy (fun ((node, _), _) -> node)
                |> Seq.map (fun (node, group) ->
                    let minComp = group |> Seq.map (fun ((_, comp), _) -> comp) |> Seq.min
                    ((node, minComp), 1))
                |> ZSet.ofSeq
                
            return minComponents
        }
        
        FixedPointOperator<'T * 'T>(componentFunc, maxIterations = 50)

/// Recursive computation patterns and utilities
module RecursivePatterns =
    
    /// Create a simple recursive operator that applies a function until stable
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let inline recursiveStep<'T when 'T: comparison and 'T: equality> 
        ([<InlineIfLambda>] func: ZSet<'T> -> Task<ZSet<'T>>) maxIter =
        new FixedPointOperator<'T>(func, maxIterations = maxIter)
    
    /// Create transitive closure operator
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let inline transitiveClosure<'T when 'T: comparison and 'T: equality> (edges: ZSet<'T * 'T>) =
        let builder = RecursiveQueryBuilder<'T>()
        builder.TransitiveClosure(edges)
    
    /// Create connected components operator
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let inline connectedComponents<'T when 'T: comparison and 'T: equality> (edges: ZSet<'T * 'T>) =
        let builder = RecursiveQueryBuilder<'T>()
        builder.ConnectedComponents(edges)

/// Iterative operator for custom recursion logic
type IterativeOperator<'T when 'T: comparison>
    ([<InlineIfLambda>] stepFunc: ZSet<'T> -> int -> Task<ZSet<'T>>, 
     ?maxSteps: int,
     ?name: string) =
    inherit BaseUnaryOperator<ZSet<'T>, ZSet<'T>>(defaultArg name "Iterative")
    
    let maxSteps = defaultArg maxSteps 100
    let mutable lastStepCount = 0
    
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    override _.EvalAsyncImpl(input: ZSet<'T>) = task {
        let mutable current = input
        let mutable step = 0
        let mutable shouldContinue = true
        
        while step < maxSteps && shouldContinue do
            let! next = stepFunc current step
            if next = current then
                // Reached fixed point
                shouldContinue <- false
            else
                current <- next
            step <- step + 1
        
        lastStepCount <- step
        return current
    }
    
    /// Get the number of steps in the last computation
    member _.LastStepCount = lastStepCount
    
    interface IUnaryOperator<ZSet<'T>, ZSet<'T>> with
        member this.EvalAsync(input) = this.EvalAsyncImpl(input)
        member _.InputPreference = NoPreference

/// Convergence detection utilities
module Convergence =
    
    /// Simple equality-based convergence check
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let inline hasConverged (previous: ZSet<'T>) (current: ZSet<'T>) =
        previous = current
    
    /// Size-based convergence check (useful for growing computations)
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let inline sizeConverged (tolerance: int) (previous: ZSet<'T>) (current: ZSet<'T>) =
        abs (current.Count - previous.Count) <= tolerance
    
    /// Weight-based convergence check (checks total weight change)
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let inline weightConverged (tolerance: int) (previous: ZSet<'T>) (current: ZSet<'T>) =
        let prevTotal = ZSet.fold (fun acc _ weight -> acc + abs weight) 0 previous
        let currTotal = ZSet.fold (fun acc _ weight -> acc + abs weight) 0 current
        abs (currTotal - prevTotal) <= tolerance
    
    /// Custom convergence predicate
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let inline customConverged ([<InlineIfLambda>] predicate: ZSet<'T> -> ZSet<'T> -> bool) =
        predicate