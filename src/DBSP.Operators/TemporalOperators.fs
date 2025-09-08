/// Temporal operators for DBSP circuits: delay, integrate, differentiate
/// These operators handle time-based transformations and accumulation
module DBSP.Operators.TemporalOperators

open System.Runtime.CompilerServices
open System.Threading.Tasks
open DBSP.Core.ZSet
open DBSP.Core.Stream
open DBSP.Operators.Interfaces

/// Delay operator (z^-1) - delays input by one time step
type DelayOperator<'T when 'T: equality>(?name: string) =
    inherit BaseUnaryOperator<'T, 'T>(defaultArg name "Delay")
    
    // Maintain state for delayed output
    let mutable previousInput: 'T option = None
    let mutable firstCall = true
    
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    override _.EvalAsyncImpl(input: 'T) = task {
        if firstCall then
            // On first call, remember input but output nothing (or zero for appropriate types)
            firstCall <- false
            previousInput <- Some input
            // For this implementation, assume we can create an appropriate "zero" value
            // In practice, this would depend on the specific type
            return input // Simplified - return input, in reality we'd need a zero value
        else
            // Return the previous input, save current for next time
            let result = previousInput.Value
            previousInput <- Some input
            return result
    }
    
    interface IUnaryOperator<'T, 'T> with
        member this.EvalAsync(input) = this.EvalAsyncImpl(input)

/// Integration operator - accumulates changes over time using ZSets
type IntegrateOperator<'T when 'T: comparison>(?name: string) =
    inherit BaseUnaryOperator<ZSet<'T>, ZSet<'T>>(defaultArg name "Integrate")
    
    // Maintain accumulated state
    let mutable accumulator: ZSet<'T> = ZSet.empty<'T>
    
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    override _.EvalAsyncImpl(input: ZSet<'T>) = task {
        // Add the input delta to the accumulator
        accumulator <- ZSet.add accumulator input
        return accumulator
    }
    
    /// Reset the accumulator (for testing purposes)
    member _.Reset() =
        accumulator <- ZSet.empty<'T>
    
    /// Get current accumulated value
    member _.CurrentValue = accumulator
    
    interface IUnaryOperator<ZSet<'T>, ZSet<'T>> with
        member this.EvalAsync(input) = this.EvalAsyncImpl(input)

/// Differentiation operator - computes differences between consecutive time steps
type DifferentiateOperator<'T when 'T: comparison>(?name: string) =
    inherit BaseUnaryOperator<ZSet<'T>, ZSet<'T>>(defaultArg name "Differentiate")
    
    // Maintain previous accumulated value
    let mutable previousAccum: ZSet<'T> = ZSet.empty<'T>
    let mutable firstCall = true
    
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    override _.EvalAsyncImpl(input: ZSet<'T>) = task {
        if firstCall then
            // On first call, the difference is the input itself
            firstCall <- false
            previousAccum <- input
            return input
        else
            // Compute difference: current - previous
            let difference = ZSet.difference input previousAccum
            previousAccum <- input
            return difference
    }
    
    /// Reset the operator state
    member _.Reset() =
        previousAccum <- ZSet.empty<'T>
        firstCall <- true
    
    interface IUnaryOperator<ZSet<'T>, ZSet<'T>> with
        member this.EvalAsync(input) = this.EvalAsyncImpl(input)

/// Generator operator - produces a stream of values
type GeneratorOperator<'T when 'T: comparison>([<InlineIfLambda>] generator: int -> 'T seq, ?name: string) =
    inherit BaseOperator(defaultArg name "Generator")
    
    let mutable currentStep = 0
    
    /// Generate values for the current time step
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    member _.GenerateAsync() = task {
        let values = generator currentStep
        currentStep <- currentStep + 1
        
        // Convert to ZSet with weight 1 for each generated value
        let zset = 
            values
            |> Seq.map (fun v -> (v, 1))
            |> ZSet.ofSeq
        
        return zset
    }
    
    /// Reset the generator to initial state
    member _.Reset() =
        currentStep <- 0
    
    /// Get current step
    member _.CurrentStep = currentStep

/// Inspect operator - observes values without modifying them (for debugging/testing)
type InspectOperator<'T>([<InlineIfLambda>] inspector: 'T -> unit, ?name: string) =
    inherit BaseUnaryOperator<'T, 'T>(defaultArg name "Inspect")
    
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    override _.EvalAsyncImpl(input: 'T) = task {
        // Call the inspector function for side effects
        inspector input
        // Return the input unchanged
        return input
    }
    
    interface IUnaryOperator<'T, 'T> with
        member this.EvalAsync(input) = this.EvalAsyncImpl(input)

/// Clock operator - provides discrete time steps for temporal operations
type ClockOperator(?name: string) =
    inherit BaseOperator(defaultArg name "Clock")
    
    let mutable currentTime: int64 = 0L
    
    /// Advance to next time step
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    member _.TickAsync() = task {
        currentTime <- currentTime + 1L
        return currentTime
    }
    
    /// Get current time
    member _.CurrentTime = currentTime
    
    /// Reset clock to time 0
    member _.Reset() =
        currentTime <- 0L

/// Convenience functions for creating temporal operators
module TemporalOperators =
    
    /// Create a delay operator
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let inline delay<'T when 'T: equality> () = new DelayOperator<'T>()
    
    /// Create an integrate operator  
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let inline integrate<'T when 'T: comparison> () = new IntegrateOperator<'T>()
    
    /// Create a differentiate operator
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let inline differentiate<'T when 'T: comparison> () = new DifferentiateOperator<'T>()
    
    /// Create a generator operator
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let inline generator<'T when 'T: comparison> ([<InlineIfLambda>] gen: int -> 'T seq) = new GeneratorOperator<'T>(gen)
    
    /// Create an inspect operator
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let inline inspect<'T> ([<InlineIfLambda>] inspector: 'T -> unit) = new InspectOperator<'T>(inspector)
    
    /// Create a clock operator
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let inline clock () = new ClockOperator()