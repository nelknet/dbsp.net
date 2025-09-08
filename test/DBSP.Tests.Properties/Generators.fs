/// Custom FsCheck generators for DBSP data types
module DBSP.Tests.Properties.Generators

open FsCheck
open DBSP.Core.ZSet

/// Generator for ZSet with integer keys
type ZSetGenerators =
    
    /// Generate a ZSet<int> with reasonable bounds
    static member ZSetInt() =
        Gen.choose(0, 20)
        |> Gen.bind (fun size ->
            Gen.listOfLength size (Gen.zip (Gen.choose(-100, 100)) (Gen.choose(-10, 10)))
            |> Gen.map (fun pairs ->
                // Filter out zero weights and combine duplicates
                pairs
                |> List.filter (fun (_, w) -> w <> 0)
                |> ZSet.ofList
            )
        )
        |> Arb.fromGen
    
    /// Generate a ZSet<string> with reasonable bounds
    static member ZSetString() =
        Gen.choose(0, 10)
        |> Gen.bind (fun size ->
            Gen.listOfLength size 
                (Gen.zip 
                    (Gen.elements ["a"; "b"; "c"; "d"; "e"; "f"; "g"; "h"])
                    (Gen.choose(-5, 5)))
            |> Gen.map (fun pairs ->
                // Filter out zero weights
                pairs
                |> List.filter (fun (_, w) -> w <> 0)
                |> ZSet.ofList
            )
        )
        |> Arb.fromGen

    /// Generate a ZSet<float> with reasonable bounds
    static member ZSetFloat() =
        Gen.choose(0, 15)
        |> Gen.bind (fun size ->
            Gen.listOfLength size 
                (Gen.zip 
                    (Gen.map (fun x -> float x / 10.0) (Gen.choose(-100, 100)))
                    (Gen.choose(-5, 5)))
            |> Gen.map (fun pairs ->
                // Filter out zero weights
                pairs
                |> List.filter (fun (_, w) -> w <> 0)
                |> ZSet.ofList
            )
        )
        |> Arb.fromGen