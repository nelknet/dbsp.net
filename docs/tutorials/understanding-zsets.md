# Understanding Z-Sets

`ZSet<'T>` is the mathematical core of DBSP. It behaves like a multiset with
integer weights, enabling additions (positive weight) and deletions (negative
weight) while remaining truly compositional.

This tutorial focuses on hands-on exploration:

- constructing `ZSet`s from scratch and from sequences
- combining them with `add`, `difference`, and `negate`
- projecting and filtering keys while preserving weights

Sample code:
[`examples/DBSP.Tutorials/UnderstandingZSets.fs`](../../examples/DBSP.Tutorials/UnderstandingZSets.fs)

## Try it yourself

```bash
dotnet run --project examples/DBSP.Tutorials -- --sample zsets
```

The program prints the results of each algebraic operation and highlights when
a key vanishes because its net weight reaches zero.

## Highlights

### Builders keep things fast

```fsharp
let z =
    ZSetDelta.Create<string>()
        .AddInsert("insert")
        .AddDelete("delete")
        .AddWeight("upsert", -1)
        .AddWeight("upsert", 1)
        .ToZSet()
```

`ZSetDelta.Create` batches intent-driven operations and coalesces duplicate
keys automatically. The example above produces an empty `ZSet` because the
positive and negative weights cancel out.

### Algebra is closed

```fsharp
let unionAB = ZSet.add a b
let delta = ZSet.difference unionAB c
let normalized = ZSet.negate delta
```

All common operations stay inside the `ZSet` type — there is no need to unpack
into raw `seq<'Key * int>` until you want to inspect results.

### Mapping and filtering

```fsharp
let projected = ZSet.mapKeys (fun (city, count) -> city) populationDelta
let positives = ZSet.filter (fun (city, weight) -> weight > 0) populationDelta
```

`mapKeys` transforms the key while keeping the weight. `filter` removes entries
that do not satisfy the predicate, useful for extracting just the insertions or
just the deletions from a delta.

## Further reading

- Apply these primitives to a circuit in
  [Building Your First Circuit](first-circuit.md).
- Join multiple `ZSet`s incrementally in
  [Incremental Joins](incremental-joins.md).
