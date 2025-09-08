/// Basic smoke tests to verify test framework setup
module DBSP.Tests.Unit.SmokeTests

open NUnit.Framework

[<SetUp>]
let Setup () =
    ()

[<Test>]
let ``Test framework is working`` () =
    Assert.Pass("Basic test framework functionality verified")