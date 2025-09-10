module DBSP.Tests.Unit.TestConfiguration

open FsCheck

/// Configure FsCheck to suppress verbose output during test runs
let QuietConfig = 
    Config.Default
        .WithMaxTest(100)
        .WithQuietOnSuccess(true)  // This suppresses "Ok, passed X tests" messages

/// Quick config for CI/fast feedback with quiet output
let QuickQuietConfig = 
    Config.Quick
        .WithQuietOnSuccess(true)