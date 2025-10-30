module Program

open System

type Options = {
    Sample: string option
    Iterations: int
    ChangesPerStep: int
}

let private defaultOptions = {
    Sample = None
    Iterations = 5
    ChangesPerStep = 1_000
}

let rec private parseArgs (args: string[]) (index: int) (options: Options) =
    if index >= args.Length then options
    else
        match args[index].ToLowerInvariant() with
        | "--sample" when index + 1 < args.Length ->
            parseArgs args (index + 2) { options with Sample = Some(args[index + 1].ToLowerInvariant()) }
        | "--iterations" when index + 1 < args.Length ->
            match Int32.TryParse(args[index + 1]) with
            | true, value -> parseArgs args (index + 2) { options with Iterations = value }
            | _ -> parseArgs args (index + 1) options
        | "--changes" when index + 1 < args.Length ->
            match Int32.TryParse(args[index + 1]) with
            | true, value -> parseArgs args (index + 2) { options with ChangesPerStep = value }
            | _ -> parseArgs args (index + 1) options
        | _ ->
            parseArgs args (index + 1) options

let private printUsage () =
    printfn "Usage: dotnet run --project examples/DBSP.Tutorials -- --sample <name> [--iterations N] [--changes N]"
    printfn "Samples:"
    printfn "  getting-started     Basic ZSet deltas with IntegrateOperator"
    printfn "  first-circuit       Circuit builder + runtime example"
    printfn "  zsets               ZSet algebra walkthrough"
    printfn "  incremental-joins   InnerJoinOperator incremental demo"
    printfn "  performance         Naive vs incremental timing harness"

[<EntryPoint>]
let main argv =
    let options = parseArgs argv 0 defaultOptions
    match options.Sample with
    | Some "getting-started" ->
        DBSP.Tutorials.GettingStarted.run ()
        0
    | Some "first-circuit" ->
        DBSP.Tutorials.FirstCircuit.run ()
        0
    | Some "zsets" ->
        DBSP.Tutorials.UnderstandingZSets.run ()
        0
    | Some "incremental-joins" ->
        DBSP.Tutorials.IncrementalJoins.run ()
        0
    | Some "performance" ->
        DBSP.Tutorials.Performance.run options.Iterations options.ChangesPerStep
        0
    | _ ->
        printUsage ()
        1
