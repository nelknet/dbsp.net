namespace DBSP.Tests.Unit

open FsCheck.NUnit

/// Global FsCheck configuration for the entire test assembly
/// This sets QuietOnSuccess to true for all property tests
[<assembly: Properties(QuietOnSuccess = true, MaxTest = 100)>]
do ()