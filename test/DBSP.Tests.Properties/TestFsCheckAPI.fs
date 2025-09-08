/// Test file to understand FsCheck 3.3.1 API
module TestFsCheckAPI

open FsCheck
open FsCheck.FSharp

// Test the correct FsCheck 3.3.1 API with FsCheck.FSharp
let testCorrectAPI = 
    printfn "Trying correct FsCheck API..."
    try
        // Use the correct FsCheck 3.3.1 API pattern
        let gen = Gen.choose(1, 10)
        let arb = Arb.fromGen gen
        printfn "FsCheck API works: gen=%A, arb=%A" gen arb
        true
    with ex ->
        printfn "FsCheck API failed: %A" ex.Message
        false

// Test computation expression
let testGenCE =
    printfn "Trying gen computation expression..."
    try
        let gen = 
            gen {
                let! x = Gen.choose(1, 10)
                return x * 2
            }
        printfn "Gen computation expression works: %A" gen
        true
    with ex ->
        printfn "Gen CE failed: %A" ex.Message
        false

let runTests () =
    printfn "=== FsCheck API Tests ==="
    printfn "Test 1 (API access): %b" testCorrectAPI
    printfn "Test 2 (Gen CE): %b" testGenCE