/// Streams: Time-indexed sequences of changes for temporal processing
module DBSP.Core.Stream

open FSharp.Data.Adaptive
open DBSP.Core.Algebra

/// Stream type representing time-indexed data  
type Stream<'T> = 
    { 
        /// Timeline mapping timestamps to values using HashMap for O(1) access
        Timeline: HashMap<int64, 'T>  
        /// Current logical time position
        mutable CurrentTime: int64 
    }
    
    // Static members with HashMap optimizations
    static member Empty : Stream<'T> = { Timeline = HashMap.empty; CurrentTime = 0L }
    
    /// Combine two streams (requires compatible 'T type with addition)
    static member Combine (stream1: Stream<'T>) (stream2: Stream<'T>) (combiner: 'T -> 'T -> 'T) : Stream<'T> =
        // O(N + M) HashMap union for efficient timeline combining
        let combinedTimeline = 
            HashMap.unionWith (fun _ value1 value2 -> 
                combiner value1 value2
            ) stream1.Timeline stream2.Timeline
        { Timeline = combinedTimeline; CurrentTime = max stream1.CurrentTime stream2.CurrentTime }

    /// Check if stream is empty (no data at any timestamp)
    member this.IsEmpty = HashMap.isEmpty this.Timeline

    /// Get value at specific timestamp
    member this.GetValueAt(time: int64) =
        HashMap.tryFind time this.Timeline

    /// Get all timestamps with data
    member this.Timestamps =
        HashMap.keys this.Timeline |> Seq.sort

    /// Get the latest timestamp with data
    member this.LatestTimestamp =
        if HashMap.isEmpty this.Timeline then
            None
        else
            HashMap.keys this.Timeline |> Seq.max |> Some

/// Module functions for Stream operations
module Stream =

    /// Create empty stream
    let empty<'T> : Stream<'T> = 
        { Timeline = HashMap.empty; CurrentTime = 0L }

    /// Create stream with single timestamp-value pair
    let singleton time value =
        { Timeline = HashMap.ofList [(time, value)]; CurrentTime = time }

    /// Create stream from sequence of (timestamp, value) pairs
    let ofSeq (pairs: seq<int64 * 'T>) =
        let timeline = HashMap.ofSeq pairs
        let maxTime = if Seq.isEmpty pairs then 0L else pairs |> Seq.map fst |> Seq.max
        { Timeline = timeline; CurrentTime = maxTime }

    /// Convert stream to sequence of (timestamp, value) pairs
    let toSeq (stream: Stream<'T>) =
        HashMap.toSeq stream.Timeline
        |> Seq.sortBy fst

    /// Combine two streams using provided combiner function
    let combine (stream1: Stream<'T>) (stream2: Stream<'T>) (combiner: 'T -> 'T -> 'T) = 
        Stream<'T>.Combine stream1 stream2 combiner

    /// Add a value at specific timestamp (requires 'T to support addition and zero)
    let insertAt time value (stream: Stream<'T>) (defaultValue: 'T) (adder: 'T -> 'T -> 'T) =
        let existingValue = HashMap.tryFind time stream.Timeline |> Option.defaultValue defaultValue
        let newValue = adder existingValue value
        let newTimeline = HashMap.add time newValue stream.Timeline
        { Timeline = newTimeline; CurrentTime = max stream.CurrentTime time }

    /// Delay stream by given time offset
    let delay (offset: int64) (stream: Stream<'T>) : Stream<'T> =
        // O(N) map operation with efficient HashMap operations
        let delayedTimeline = 
            HashMap.toSeq stream.Timeline
            |> Seq.map (fun (time, value) -> (time + offset, value))
            |> HashMap.ofSeq
        { Timeline = delayedTimeline; CurrentTime = stream.CurrentTime + offset }

    /// Integrate stream (compute cumulative values over time) with provided zero value and adder
    let integrate (stream: Stream<'T>) (zeroValue: 'T) (adder: 'T -> 'T -> 'T) : Stream<'T> =
        // O(N log N) sorting required for temporal integration
        let sortedEntries = 
            HashMap.toSeq stream.Timeline
            |> Seq.sortBy fst
            |> Seq.toArray
        
        if Array.isEmpty sortedEntries then
            empty<'T>
        else
            let mutable accumulator = zeroValue
            let integratedEntries = 
                sortedEntries
                |> Array.map (fun (time, value) ->
                    accumulator <- adder accumulator value
                    (time, accumulator)
                )
            
            { Timeline = HashMap.ofArray integratedEntries; CurrentTime = stream.CurrentTime }

    /// Filter stream by time predicate
    let filterByTime predicate stream =
        let filteredTimeline = HashMap.filter (fun time _ -> predicate time) stream.Timeline
        { stream with Timeline = filteredTimeline }

    /// Filter stream by value predicate
    let filterByValue predicate stream =
        let filteredTimeline = HashMap.filter (fun _ value -> predicate value) stream.Timeline
        { stream with Timeline = filteredTimeline }

    /// Map over stream values (preserving timestamps)
    let mapValues f stream =
        let mappedTimeline = HashMap.map (fun _ value -> f value) stream.Timeline
        { stream with Timeline = mappedTimeline }