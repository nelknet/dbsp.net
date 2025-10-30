module DBSP.Tutorials.GettingStarted

open DBSP.Core.ZSet
open DBSP.Operators.TemporalOperators
open DBSP.Tutorials.Common

let private mkDelta (pairs: (string * int) list) =
    ZSet.buildWith (fun builder ->
        for (key, weight) in pairs do
            builder.Add(key, weight))

let private sampleDeltas =
    [ mkDelta [ "alice", 1; "bob", 1 ]
      mkDelta [ "alice", -1; "charlie", 1 ]
      mkDelta [ "bob", -1 ]
      mkDelta [ "charlie", -1; "alice", 1 ] ]

let run () =
    printHeader "Getting Started — Integrate deltas"
    let integrate = new IntegrateOperator<string>()

    sampleDeltas
    |> List.iteri (fun step delta ->
        let snapshot = integrate.EvalAsyncImpl(delta) |> runTask
        printStep step "delta " (formatZSet delta id)
        printStep step "state " (formatZSet snapshot id))
