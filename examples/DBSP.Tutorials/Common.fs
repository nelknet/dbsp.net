module DBSP.Tutorials.Common

open System
open System.Threading.Tasks
open DBSP.Core.ZSet
open DBSP.Core.IndexedZSet

let inline runTask (task: Task<'T>) = task.GetAwaiter().GetResult()

let inline formatWeight (weight: int) =
    if weight >= 0 then $"+{weight}" else string weight

let formatZSet (zset: ZSet<'K>) (formatKey: 'K -> string) =
    zset
    |> ZSet.fold (fun acc key weight ->
        if weight <> 0 then
            let formatted = $"{formatKey key}, {formatWeight weight}"
            formatted :: acc
        else acc) []
    |> List.sort
    |> String.concat "; "

let formatIndexedZSet (indexed: IndexedZSet<'K,'V>) (formatKey: 'K -> string) (formatValue: 'V -> string) =
    indexed
    |> IndexedZSet.toSeq
    |> Seq.sortBy (fun (key, _, _) -> key)
    |> Seq.map (fun (key, value, weight) -> $"{formatKey key}, {formatValue value}, {formatWeight weight}")
    |> String.concat "; "

let printHeader title =
    printfn "\n=== %s ===" title

let printStep step label message =
    printfn "step %d %s -> %s" step label message
