module DBSP.Tests.Properties.TestConfiguration

open FsCheck
open System

/// Configure FsCheck to suppress verbose output during test runs
let QuietConfig = 
    Config.Default
        .WithMaxTest(100)
        .WithQuietOnSuccess(true)  // This suppresses "Ok, passed X tests" messages

/// Quick config for CI/fast feedback
let QuickConfig = 
    Config.Default
        .WithMaxTest(30)
        .WithQuietOnSuccess(true)

/// Stress config for thorough testing
let StressConfig = 
    Config.Default
        .WithMaxTest(500)
        .WithQuietOnSuccess(true)

/// Configure console output redirection for tests
type OutputRedirector() =
    let originalOut = Console.Out
    let originalError = Console.Error
    
    member _.Suppress() =
        Console.SetOut(IO.TextWriter.Null)
        Console.SetError(IO.TextWriter.Null)
    
    member _.Restore() =
        Console.SetOut(originalOut)
        Console.SetError(originalError)
    
    interface IDisposable with
        member this.Dispose() = this.Restore()