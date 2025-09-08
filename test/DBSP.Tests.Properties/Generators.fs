/// Custom FsCheck generators for DBSP data types
module DBSP.Tests.Properties.Generators

open FsCheck
open FsCheck.FSharp
open DBSP.Core.ZSet

/// Generator for ZSet with integer keys
type ZSetGenerators =
    
    /// Generate a ZSet<int> with reasonable bounds
    static member ZSetInt() =
        let genZSet = 
            gen {
                let! size = Gen.choose(0, 20)
                let! pairs = Gen.listOfLength size (Gen.zip (Gen.choose(-100, 100)) (Gen.choose(-10, 10)))
                
                // Filter out zero weights and combine duplicates
                let zset = 
                    pairs
                    |> List.filter (fun (_, w) -> w <> 0)
                    |> ZSet.ofList
                    
                return zset
            }
        Arb.fromGen genZSet
    
    /// Generate a ZSet<string> with reasonable bounds
    static member ZSetString() =
        let genZSet = 
            gen {
                let! size = Gen.choose(0, 10)
                let! pairs = 
                    Gen.listOfLength size 
                        (Gen.zip 
                            (Gen.elements ["a"; "b"; "c"; "d"; "e"; "f"; "g"; "h"])
                            (Gen.choose(-5, 5)))
                
                // Filter out zero weights
                let zset = 
                    pairs
                    |> List.filter (fun (_, w) -> w <> 0)
                    |> ZSet.ofList
                    
                return zset
            }
        Arb.fromGen genZSet

    /// Generate a ZSet<float> with reasonable bounds
    static member ZSetFloat() =
        let genZSet = 
            gen {
                let! size = Gen.choose(0, 15)
                let! pairs = 
                    Gen.listOfLength size 
                        (Gen.zip 
                            (Gen.map (fun x -> float x / 10.0) (Gen.choose(-100, 100)))
                            (Gen.choose(-5, 5)))
                
                // Filter out zero weights
                let zset = 
                    pairs
                    |> List.filter (fun (_, w) -> w <> 0)
                    |> ZSet.ofList
                    
                return zset
            }
        Arb.fromGen genZSet