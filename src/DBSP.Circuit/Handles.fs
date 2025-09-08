namespace DBSP.Circuit

open System
open System.Collections.Generic
open System.Runtime.CompilerServices
open System.Threading.Channels
open System.Threading.Tasks

/// Channel configuration for input/output handles
type ChannelConfig = {
    Capacity: int
    AllowUnbounded: bool
    SingleReader: bool
    SingleWriter: bool
} with
    static member Default = {
        Capacity = 1000
        AllowUnbounded = false
        SingleReader = true
        SingleWriter = false
    }

/// Simple input handle for external data ingestion
type SimpleInputHandle<'T> = {
    Writer: ChannelWriter<'T>
    Reader: ChannelReader<'T>
    mutable IsConnected: bool
} with
    /// Send single value asynchronously
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    member this.SendAsync(value: 'T) = task {
        if this.IsConnected then
            try
                do! this.Writer.WriteAsync(value).AsTask()
                return Ok ()
            with
            | ex -> return Error ex.Message
        else
            return Error "Not connected"
    }
    
    /// Complete input
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    member this.Complete() =
        this.IsConnected <- false
        this.Writer.Complete()

/// Simple output handle for consuming results
type SimpleOutputHandle<'T> = {
    Writer: ChannelWriter<'T>
    Reader: ChannelReader<'T>
    mutable CurrentValue: 'T option
} with
    /// Publish value
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    member this.PublishAsync(value: 'T) = task {
        this.CurrentValue <- Some value
        try
            do! this.Writer.WriteAsync(value).AsTask()
        with
        | _ -> ()
    }
    
    /// Get current value
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    member this.GetCurrentValue() = this.CurrentValue

module HandleFactoryModule =
    /// Create input handle with default configuration
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let createInput<'T> (name: string) : SimpleInputHandle<'T> =
        let channel = Channel.CreateBounded<'T>(1000)
        {
            Writer = channel.Writer
            Reader = channel.Reader
            IsConnected = true
        }
    
    /// Create output handle with default configuration
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let createOutput<'T> (name: string) : SimpleOutputHandle<'T> =
        let channel = Channel.CreateBounded<'T>(1000)
        {
            Writer = channel.Writer
            Reader = channel.Reader
            CurrentValue = None
        }