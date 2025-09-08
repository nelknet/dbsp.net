/// Core operator interfaces for DBSP circuits
/// Based on Feldera's async operator evaluation model with Task-based execution
module DBSP.Operators.Interfaces

open System.Threading.Tasks

/// Ownership preference for input data (for optimization)
[<Struct>]
type OwnershipPreference =
    | PreferRef     // Prefer reference to avoid copying
    | PreferOwned   // Prefer owned value for mutation
    | NoPreference  // Either is fine

/// Scope for circuit execution phases  
[<Struct>]
type Scope = 
    | Clock of int64
    | Step of int64

/// Base operator interface providing common functionality
type IOperator =
    /// Human-readable name for debugging and visualization
    abstract member Name : string
    
    /// Whether this operator performs async I/O operations
    abstract member IsAsync : bool
    
    /// Whether the operator is ready to execute (for async coordination)
    abstract member Ready : bool
    
    /// Check if operator has reached fixed-point for this scope
    abstract member Fixedpoint : Scope -> bool
    
    /// Flush any pending operations
    abstract member Flush : unit -> unit
    
    /// Clear internal state (for reset/restart scenarios)
    abstract member ClearState : unit -> unit

/// Unary operator interface for single-input operators (Map, Filter, Aggregate)
type IUnaryOperator<'I, 'O> =
    inherit IOperator
    
    /// Async evaluation of input to output
    abstract member EvalAsync : input:'I -> Task<'O>
    
    /// Input ownership preference for performance optimization
    abstract member InputPreference : OwnershipPreference

/// Binary operator interface for two-input operators (Join, Union, Minus)  
type IBinaryOperator<'I1, 'I2, 'O> =
    inherit IOperator
    
    /// Async evaluation of two inputs to output
    abstract member EvalAsync : left:'I1 -> right:'I2 -> Task<'O>
    
    /// Input ownership preferences for both inputs
    abstract member InputPreferences : OwnershipPreference * OwnershipPreference

/// Source operator interface for data generators and input handles
type ISourceOperator<'O> =
    inherit IOperator
    
    /// Generate output without input
    abstract member EvalAsync : unit -> Task<'O>

/// Sink operator interface for data consumers and output handles
type ISinkOperator<'I> =
    inherit IOperator
    
    /// Consume input without producing output
    abstract member EvalAsync : input:'I -> Task<unit>

/// Stateful operator support for incremental computation
type IStatefulOperator<'State> =
    inherit IOperator
    
    /// Get current state for checkpointing
    abstract member GetState : unit -> 'State
    
    /// Set state for restoration
    abstract member SetState : 'State -> unit
    
    /// Serialize state for persistence
    abstract member SerializeState : unit -> Task<byte[]>
    
    /// Deserialize state from persistence
    abstract member DeserializeState : byte[] -> Task<unit>

/// Base operator implementation providing common functionality
[<AbstractClass>]
type BaseOperator(name: string) =
    let mutable ready = true
    let mutable fixedpointReached = false
    
    interface IOperator with
        member _.Name = name
        member _.IsAsync = false  // Override in async operators
        member _.Ready = ready
        member _.Fixedpoint(_scope) = fixedpointReached
        member _.Flush() = () // Override if needed
        member _.ClearState() = 
            ready <- true
            fixedpointReached <- false
    
    /// Set operator readiness state
    member _.SetReady(value: bool) = ready <- value
    
    /// Set fixed-point state
    member _.SetFixedpoint(value: bool) = fixedpointReached <- value

/// Base unary operator providing default implementations
[<AbstractClass>]
type BaseUnaryOperator<'I, 'O>(name: string) =
    inherit BaseOperator(name)
    
    interface IUnaryOperator<'I, 'O> with
        member this.EvalAsync(input: 'I) = this.EvalAsyncImpl(input)
        member _.InputPreference = NoPreference
    
    /// Override this method in concrete operators
    abstract member EvalAsyncImpl : 'I -> Task<'O>

/// Base binary operator providing default implementations  
[<AbstractClass>]
type BaseBinaryOperator<'I1, 'I2, 'O>(name: string) =
    inherit BaseOperator(name)
    
    interface IBinaryOperator<'I1, 'I2, 'O> with
        member this.EvalAsync left right = this.EvalAsyncImpl left right
        member _.InputPreferences = (NoPreference, NoPreference)
    
    /// Override this method in concrete operators
    abstract member EvalAsyncImpl : 'I1 -> 'I2 -> Task<'O>

/// Operator factory for creating operator instances with configuration
type OperatorFactory<'Config, 'Op when 'Op :> IOperator> = {
    CreateOperator: 'Config -> 'Op
    OperatorType: string
    DefaultConfig: 'Config
}

/// Operator metadata for circuit construction and visualization
[<Struct>]
type OperatorMetadata = {
    Id: int64
    Name: string
    OperatorType: string
    InputTypes: System.Type list
    OutputType: System.Type
    IsAsync: bool
    IsStateful: bool
}