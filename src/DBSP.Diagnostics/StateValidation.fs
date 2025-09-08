/// State machine validation for circuit execution
/// Based on Feldera's scheduler event validation automaton
module DBSP.Diagnostics.StateValidation

open System
open System.Collections.Concurrent
open DBSP.Diagnostics.CircuitEvents

/// Circuit execution states
type CircuitState =
    | Idle
    | Running
    | Step of visitedNodes: Set<GlobalNodeId>
    | Eval of visitedNodes: Set<GlobalNodeId> * currentNode: GlobalNodeId
    | Wait

/// State validation context for a single circuit
type CircuitValidationContext = {
    CircuitId: int64
    CurrentState: CircuitState
    StateHistory: (CircuitState * DateTime) list
    ExpectedNodes: Set<GlobalNodeId>  // Nodes that should be evaluated
    EvaluatedNodes: Set<GlobalNodeId> // Nodes that have been evaluated
    LastStateChange: DateTime
}

/// Global state validator tracking all circuits
type StateValidator = {
    /// Per-circuit validation contexts
    CircuitContexts: ConcurrentDictionary<int64, CircuitValidationContext>
    /// Global execution state
    mutable GlobalState: CircuitState
    /// State transition errors
    mutable ValidationErrors: TraceError list
    /// Clock nesting level
    mutable ClockLevel: int
}

/// State validator operations
module StateValidator =
    
    /// Create new state validator
    let create () = {
        CircuitContexts = ConcurrentDictionary<int64, CircuitValidationContext>()
        GlobalState = Idle
        ValidationErrors = []
        ClockLevel = 0
    }
    
    /// Get or create validation context for a circuit
    let getOrCreateContext (validator: StateValidator) (circuitId: int64) (expectedNodes: Set<GlobalNodeId>) =
        validator.CircuitContexts.GetOrAdd(circuitId, fun _ -> {
            CircuitId = circuitId
            CurrentState = Idle
            StateHistory = []
            ExpectedNodes = expectedNodes
            EvaluatedNodes = Set.empty
            LastStateChange = DateTime.UtcNow
        })
    
    /// Validate state transition for a circuit
    let validateTransition (validator: StateValidator) (circuitId: int64) (newState: CircuitState) (event: SchedulerEvent) =
        let context = getOrCreateContext validator circuitId Set.empty
        let currentTime = DateTime.UtcNow
        
        let isValidTransition = 
            match context.CurrentState, newState with
            // Valid transitions from Idle
            | Idle, Running -> true
            | Idle, Step _ -> true
            
            // Valid transitions from Running  
            | Running, Step _ -> true
            | Running, Wait -> true
            | Running, Idle -> true
            
            // Valid transitions from Step
            | Step _, Eval _ -> true
            | Step _, Running -> true
            | Step _, Wait -> true
            
            // Valid transitions from Eval
            | Eval _, Step _ -> true
            | Eval _, Running -> true
            | Eval _, Wait -> true
            
            // Valid transitions from Wait
            | Wait, Running -> true
            | Wait, Idle -> true
            
            // Self-transitions (updates within same state)
            | Step s1, Step s2 -> true
            | Eval _, Eval _ -> true
            
            // Invalid transitions
            | _ -> false
        
        if isValidTransition then
            let updatedContext = {
                context with
                    CurrentState = newState
                    StateHistory = (context.CurrentState, currentTime) :: context.StateHistory
                    LastStateChange = currentTime
            }
            validator.CircuitContexts.[circuitId] <- updatedContext
            Ok updatedContext
        else
            let error = TraceError.InvalidStateTransition(
                string context.CurrentState,
                string newState,
                event
            )
            validator.ValidationErrors <- error :: validator.ValidationErrors
            Error error
    
    /// Process scheduler event and validate state transitions
    let processEvent (validator: StateValidator) (event: SchedulerEvent) =
        match event with
        | ClockStart ->
            validator.ClockLevel <- validator.ClockLevel + 1
            match validator.GlobalState with
            | Idle -> 
                validator.GlobalState <- Running
                Ok ()
            | _ ->
                let error = TraceError.InvalidStateTransition(string validator.GlobalState, "Running", event)
                validator.ValidationErrors <- error :: validator.ValidationErrors
                Error error
        
        | ClockEnd ->
            validator.ClockLevel <- max 0 (validator.ClockLevel - 1)
            validator.GlobalState <- Idle
            Ok ()
        
        | StepStart circuitId ->
            let newState = Step(Set.empty)
            validateTransition validator circuitId newState event
            |> Result.map ignore
        
        | StepEnd circuitId ->
            let context = getOrCreateContext validator circuitId Set.empty
            match context.CurrentState with
            | Step visitedNodes ->
                // Validate that all expected nodes were evaluated
                if not (Set.isEmpty context.ExpectedNodes) && 
                   not (Set.isSubset context.ExpectedNodes context.EvaluatedNodes) then
                    let missing = Set.difference context.ExpectedNodes context.EvaluatedNodes
                    let error = TraceError.InvalidEvent($"Step completed but missing nodes: {missing}")
                    validator.ValidationErrors <- error :: validator.ValidationErrors
                    Error error
                else
                    validateTransition validator circuitId Running event
                    |> Result.map ignore
            | _ ->
                let error = TraceError.InvalidStateTransition(string context.CurrentState, "Running", event)
                validator.ValidationErrors <- error :: validator.ValidationErrors
                Error error
        
        | EvalStart nodeId ->
            let circuitId = nodeId.CircuitId
            let context = getOrCreateContext validator circuitId Set.empty
            match context.CurrentState with
            | Step visitedNodes ->
                let newVisitedNodes = Set.add nodeId visitedNodes
                let newState = Eval(newVisitedNodes, nodeId)
                validateTransition validator circuitId newState event
                |> Result.map ignore
            | _ ->
                let error = TraceError.InvalidStateTransition(string context.CurrentState, $"Eval({nodeId})", event)
                validator.ValidationErrors <- error :: validator.ValidationErrors
                Error error
        
        | EvalEnd nodeId ->
            let circuitId = nodeId.CircuitId
            let context = getOrCreateContext validator circuitId Set.empty
            match context.CurrentState with
            | Eval(visitedNodes, currentNode) when currentNode = nodeId ->
                let updatedEvaluated = Set.add nodeId context.EvaluatedNodes
                let updatedContext = { context with EvaluatedNodes = updatedEvaluated }
                validator.CircuitContexts.[circuitId] <- updatedContext
                
                let newState = Step(visitedNodes)
                validateTransition validator circuitId newState event
                |> Result.map ignore
            | _ ->
                let error = TraceError.InvalidStateTransition(string context.CurrentState, $"Step", event)
                validator.ValidationErrors <- error :: validator.ValidationErrors
                Error error
        
        | WaitStart circuitId ->
            validateTransition validator circuitId Wait event
            |> Result.map ignore
        
        | WaitEnd circuitId ->
            let context = getOrCreateContext validator circuitId Set.empty
            match context.CurrentState with
            | Wait -> 
                validateTransition validator circuitId Running event
                |> Result.map ignore
            | _ ->
                let error = TraceError.InvalidStateTransition(string context.CurrentState, "Running", event)
                validator.ValidationErrors <- error :: validator.ValidationErrors
                Error error
    
    /// Set expected nodes for a circuit (for validation)
    let setExpectedNodes (validator: StateValidator) (circuitId: int64) (nodes: Set<GlobalNodeId>) =
        let context = getOrCreateContext validator circuitId nodes
        let updatedContext = { context with ExpectedNodes = nodes }
        validator.CircuitContexts.[circuitId] <- updatedContext
    
    /// Get current state of a circuit
    let getCurrentState (validator: StateValidator) (circuitId: int64) =
        match validator.CircuitContexts.TryGetValue(circuitId) with
        | true, context -> Some context.CurrentState
        | false, _ -> None
    
    /// Get validation errors
    let getErrors (validator: StateValidator) = validator.ValidationErrors
    
    /// Clear validation errors
    let clearErrors (validator: StateValidator) =
        validator.ValidationErrors <- []
    
    /// Get all circuit contexts
    let getAllContexts (validator: StateValidator) =
        validator.CircuitContexts.Values |> Seq.toList
    
    /// Validate that all circuits are in valid final state
    let validateFinalState (validator: StateValidator) =
        let mutable errors = []
        
        // Check global state
        if validator.GlobalState <> Idle then
            errors <- TraceError.InvalidEvent($"Global state should be Idle, but is {validator.GlobalState}") :: errors
        
        // Check circuit states
        for context in validator.CircuitContexts.Values do
            match context.CurrentState with
            | Idle | Running -> () // Valid final states
            | _ -> 
                errors <- TraceError.InvalidEvent($"Circuit {context.CircuitId} in invalid final state: {context.CurrentState}") :: errors
        
        if List.isEmpty errors then Ok () else Error errors
    
    /// Reset validator to initial state
    let reset (validator: StateValidator) =
        validator.CircuitContexts.Clear()
        validator.GlobalState <- Idle
        validator.ValidationErrors <- []
        validator.ClockLevel <- 0