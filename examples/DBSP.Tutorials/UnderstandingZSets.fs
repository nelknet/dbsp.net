module DBSP.Tutorials.UnderstandingZSets

open DBSP.Core.ZSet
open DBSP.Tutorials.Common

let private mkZSet (pairs: ('K * int) list) =
    ZSet.buildWith (fun builder ->
        for (key, weight) in pairs do
            builder.Add(key, weight))

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
        ZSet.buildWith (fun builder ->
            builder.Add(("widget", 3), 1)
            builder.Add(("widget", 3), -1)
            builder.Add(("widget", 2), 1)
            builder.Add(("gizmo", 1), 1)
            builder.Add(("gizmo", 1), -1)
            builder.Add(("gizmo", 4), 1))

    printfn "\nraw inventory  -> %s" (formatZSet inventoryDelta formatTuple)

    let normalized = ZSet.mapKeys fst inventoryDelta
    printfn "projected      -> %s" (formatZSet normalized id)

    let positiveOnly = ZSet.filter (fun (_, quantity) -> quantity > 0) inventoryDelta
    printfn "positive slice -> %s" (formatZSet positiveOnly formatTuple)
