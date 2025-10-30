# Incremental Joins

DBSP joins process only the changes since the previous step. This tutorial shows
how to use the built-in `InnerJoinOperator` to maintain a customer/order join,
emitting deltas in the style of `ZSet`.

Code walkthrough:
[`examples/DBSP.Tutorials/IncrementalJoins.fs`](../../examples/DBSP.Tutorials/IncrementalJoins.fs)

## Run the demo

```bash
dotnet run --project examples/DBSP.Tutorials -- --sample incremental-joins
```

You should see output similar to:

```
step 0 -> [(c1,(widget,2)); (c2,(gizmo,1))]
step 1 -> [(c1,(widget,1)); (c1,(gizmo,1)); (c3,(gizmo,1))]
step 2 -> [(c1,(widget,1)); (c3,(gizmo,1))]
```

Each line is the delta produced by the join for that step, formatted as
`[(customerId,(product,weight))]`.

## Core pieces

### 1. Maintain indexed state implicitly

```fsharp
let join = InnerJoinOperator<string, string, string>()
let! resultDelta = join.EvalAsync leftDelta rightDelta
```

`InnerJoinOperator` stores previous inputs internally. Passing only the deltas
is enough — the operator updates its state and returns the current join delta.

### 2. Inspecting the delta

```fsharp
let asTuples =
    resultDelta
    |> IndexedZSet.toSeq
    |> Seq.map (fun (customer, (product, weight)) -> ...)
```

`IndexedZSet.toSeq` turns the result into `(key, value, weight)` triples that are
easy to print or feed into downstream operators.

### 3. Focus on change propagation

The sample sends three steps’ worth of customer and order updates:

- inserts for two customers and two products
- a product change (delete + insert)
- a customer churn (delete) and a new join partner

In every case the join work is proportional to the delta sizes, not the total
history of customers or orders.

## What’s next?

- Optimise end-to-end pipelines with
  [Performance Optimization](performance.md).
- Revisit the naive vs incremental benchmark in
  `examples/DBSP.Examples/Program.fs` for a larger walkthrough.
