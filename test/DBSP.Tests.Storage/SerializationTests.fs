module DBSP.Tests.Storage.SerializationTests

open NUnit.Framework
open DBSP.Storage

type TestRecord = { Id:int; Name:string; Value:float }

[<Test>]
let ``MessagePackSerializer roundtrips record`` () =
    let ser = MessagePackSerializer<TestRecord>() :> ISerializer<TestRecord>
    let recd = { Id = 1; Name = "Test"; Value = 3.14 }
    let bytes = ser.Serialize recd
    let recd2 = ser.Deserialize bytes
    Assert.That(recd2, Is.EqualTo recd)

[<Test>]
let ``EstimateSize matches actual serialized length`` () =
    let ser = MessagePackSerializer<TestRecord>() :> ISerializer<TestRecord>
    let recd = { Id = 42; Name = "Size"; Value = 1.23 }
    let estimated = ser.EstimateSize recd
    let actual = ser.Serialize recd |> Array.length
    Assert.That(actual, Is.EqualTo estimated)

type CustomSerializer<'T>() =
    interface ISerializer<'T> with
        member _.Serialize(value: 'T) =
            let s = if isNull (box value) then "null" else value.ToString()
            System.Text.Encoding.UTF8.GetBytes(s)
        member _.Deserialize(_bytes: byte[]) =
            failwith "Not implemented for test"
        member _.EstimateSize(value: 'T) =
            if isNull (box value) then 4 else (value.ToString().Length)

[<Test>]
let ``SerializerFactory returns working default serializer`` () =
    let ser = SerializerFactory.GetDefault<TestRecord>()
    let recd = { Id = 7; Name = "X"; Value = 0.5 }
    let bytes = ser.Serialize recd
    let back = ser.Deserialize bytes
    Assert.That(back, Is.EqualTo recd)

[<Test>]
let ``SerializerFactory honors custom factory`` () =
    let custom = CustomSerializer<TestRecord>() :> ISerializer<TestRecord>
    SerializerFactory.SetDefaultFactory(System.Func<_>(fun () -> custom))
    try
        let got = SerializerFactory.GetDefault<TestRecord>()
        Assert.That(got, Is.SameAs custom)
    finally
        // reset to messagepack for this type
        SerializerFactory.SetDefaultFactory(System.Func<_>(fun () -> SerializerFactory.CreateMessagePack<TestRecord>()))

[<Test>]
let ``MessagePackSerializer handles option None`` () =
    let ser = MessagePackSerializer<TestRecord option>() :> ISerializer<TestRecord option>
    let bytes = ser.Serialize None
    let back = ser.Deserialize bytes
    Assert.That(back, Is.EqualTo None)

[<Test>]
let ``MessagePackSerializer roundtrips list of records`` () =
    let ser = MessagePackSerializer<TestRecord list>() :> ISerializer<TestRecord list>
    let lst = [ { Id = 1; Name = "First"; Value = 1.1 }
                { Id = 2; Name = "Second"; Value = 2.2 }
                { Id = 3; Name = "Third"; Value = 3.3 } ]
    let bytes = ser.Serialize lst
    let back = ser.Deserialize bytes
    Assert.That(back, Is.EqualTo lst)
