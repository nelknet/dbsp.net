namespace DBSP.Storage

open System
open MessagePack
open MessagePack.FSharp
open MessagePack.Resolvers

/// Pluggable serialization interface
type ISerializer<'T> =
    abstract member Serialize: value:'T -> byte[]
    abstract member Deserialize: bytes:byte[] -> 'T
    abstract member EstimateSize: value:'T -> int

/// MessagePack implementation of ISerializer
type MessagePackSerializer<'T>() =
    static let options =
        MessagePackSerializerOptions.Standard
            .WithResolver(
                CompositeResolver.Create(
                    // Order matters: StandardResolver handles ValueTuple and primitives
                    StandardResolver.Instance,
                    FSharpResolver.Instance,
                    StandardResolverAllowPrivate.Instance,
                    ContractlessStandardResolver.Instance
                )
            )
    
    interface ISerializer<'T> with
        member _.Serialize(value: 'T) =
            MessagePack.MessagePackSerializer.Serialize(value, options)
        
        member _.Deserialize(bytes: byte[]) =
            MessagePack.MessagePackSerializer.Deserialize<'T>(ReadOnlyMemory(bytes), options)
        
        member _.EstimateSize(value: 'T) =
            let bytes = MessagePack.MessagePackSerializer.Serialize(value, options)
            bytes.Length

    // C#-friendly instance methods
    member this.Serialize(value: 'T) = (this :> ISerializer<'T>).Serialize(value)
    member this.Deserialize(bytes: byte[]) = (this :> ISerializer<'T>).Deserialize(bytes)
    member this.EstimateSize(value: 'T) = (this :> ISerializer<'T>).EstimateSize(value)

/// Preconfigured compressed serializer (LZ4)
type MessagePackCompressedSerializer<'T>() =
    static let options =
        MessagePackSerializerOptions.Standard
            .WithResolver(
                CompositeResolver.Create(
                    StandardResolver.Instance,
                    FSharpResolver.Instance,
                    StandardResolverAllowPrivate.Instance,
                    ContractlessStandardResolver.Instance
                )
            )
            .WithCompression(MessagePackCompression.Lz4BlockArray)

    interface ISerializer<'T> with
        member _.Serialize(value: 'T) =
            MessagePack.MessagePackSerializer.Serialize(value, options)
        member _.Deserialize(bytes: byte[]) =
            MessagePack.MessagePackSerializer.Deserialize<'T>(ReadOnlyMemory(bytes), options)
        member _.EstimateSize(value: 'T) =
            let bytes = MessagePack.MessagePackSerializer.Serialize(value, options)
            bytes.Length

/// Factory for creating serializers
module SerializerFactory =
    open System.Collections.Concurrent
    let createMessagePack<'T>() : ISerializer<'T> = MessagePackSerializer<'T>() :> ISerializer<'T>
    let private factories = ConcurrentDictionary<System.Type, obj>()
    let setDefaultFactory<'T> (factory: unit -> ISerializer<'T>) = factories[typeof<'T>] <- box factory
    let getDefault<'T>() : ISerializer<'T> =
        match factories.TryGetValue(typeof<'T>) with
        | true, f -> let g = unbox<unit -> ISerializer<'T>> f in g()
        | _ -> createMessagePack<'T>()

/// C#-friendly static wrapper
[<AbstractClass; Sealed>]
type SerializerFactory =
    class
    end
    with
        static member CreateMessagePack<'T>() : ISerializer<'T> = SerializerFactory.createMessagePack<'T>()
        static member CreateMessagePackCompressed<'T>() : ISerializer<'T> = MessagePackCompressedSerializer<'T>() :> ISerializer<'T>
        static member SetDefaultFactory<'T>(factory: System.Func<ISerializer<'T>>) = SerializerFactory.setDefaultFactory<'T> (fun () -> factory.Invoke())
        static member GetDefault<'T>() : ISerializer<'T> = SerializerFactory.getDefault<'T>()
