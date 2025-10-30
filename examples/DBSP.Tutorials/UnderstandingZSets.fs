module DBSP.Tutorials.UnderstandingZSets

open DBSP.Core
open DBSP.Core.ZSet
open DBSP.Tutorials.Common

let private mkZSet (pairs: ('K * int) list) =
    let builder = ZSetDelta.Create<'K>()
    pairs
    |> List.iter (fun (key, weight) -> builder.AddWeight(key, weight) |> ignore)
    builder.ToZSet()

let private formatTuple (name: string, count: int) = $"{name}:{count}"

let run () =
    printHeader "Understanding Z-Sets"

    let inserts = mkZSet [ "seattle", 1; "portland", 1 ]
    let adjustments = mkZSet [ "seattle", -1; "san-jose", 1 ]

    let union = ZSet.add inserts adjustments
    printfn "union          -> %s" (formatZSet union id)

    let removals = mkZSet [ "portland", 1 ]
    let remaining = ZSet.difference union removals
    printfn "difference     -> %s" (formatZSet remaining id)

    let inverted = ZSet.negate adjustments
    printfn "negate         -> %s" (formatZSet inverted id)

    let inventoryDelta =
        ZSetDelta.Create<string * int>()
            .AddWeight(("widget", 3), 1)
            .AddWeight(("widget", 3), -1)
            .AddWeight(("widget", 2), 1)
            .AddWeight(("gizmo", 1), 1)
            .AddWeight(("gizmo", 1), -1)
            .AddWeight(("gizmo", 4), 1)
            .ToZSet()

    printfn "\nraw inventory  -> %s" (formatZSet inventoryDelta formatTuple)

    let normalized = ZSet.mapKeys fst inventoryDelta
    printfn "projected      -> %s" (formatZSet normalized id)

    let positiveOnly = ZSet.filter (fun (_, quantity) -> quantity > 0) inventoryDelta
    printfn "positive slice -> %s" (formatZSet positiveOnly formatTuple)
