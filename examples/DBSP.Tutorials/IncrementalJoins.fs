module DBSP.Tutorials.IncrementalJoins

open DBSP.Core
open DBSP.Core.ZSet
open DBSP.Operators.JoinOperators
open DBSP.Tutorials.Common

type JoinStep = {
    Left: ZSet<string * string>
    Right: ZSet<string * string>
    Note: string
}

let private mkZSet (entries: ((string * string) * int) list) =
    let builder = ZSetDelta.Create<string * string>()
    entries
    |> List.iter (fun ((key, value), weight) -> builder.AddWeight((key, value), weight) |> ignore)
    builder.ToZSet()

let private steps : JoinStep list =
    [ { Note = "initial inserts"
        Left = mkZSet [ (("c1", "widget"), 1); (("c2", "gizmo"), 1) ]
        Right = mkZSet [ (("c1", "processing"), 1); (("c2", "pending"), 1) ] }
      { Note = "c1 changes product; c3 enters shipping"
        Left = mkZSet [ (("c1", "widget"), -1); (("c1", "gizmo"), 1) ]
        Right = mkZSet [ (("c3", "pending"), 1) ] }
      { Note = "c2 cancels order; c3 receives shipment"
        Left = mkZSet [ (("c2", "gizmo"), -1) ]
        Right = mkZSet [ (("c3", "pending"), -1); (("c3", "shipped"), 1) ] } ]

let run () =
    printHeader "Incremental Joins"
    let join = new InnerJoinOperator<string, string, string>()

    steps
    |> List.iteri (fun idx step ->
        let delta = join.EvalAsyncImpl step.Left step.Right |> runTask
        let formatted =
            formatIndexedZSet delta id (fun (product, status) -> $"{product}:{status}")
        printfn "step %d (%s) -> %s" idx step.Note formatted)
