# Getting Started with DBSP.NET

This tutorial walks through the smallest possible DBSP.NET workflow: materialising
per-customer order counts by applying **deltas** to a `ZSet` and integrating them
over time. Along the way you will learn how to:

- model changes as weighted elements inside a `ZSet`
- accumulate those changes with the `IntegrateOperator`
- inspect the resulting state after each step

The complete sample code lives in
[`examples/DBSP.Tutorials/GettingStarted.fs`](../../examples/DBSP.Tutorials/GettingStarted.fs).
You can run it directly via `dotnet run`.

## Prerequisites

- .NET 9 SDK (required by the repository `global.json`)
- a terminal inside the repository root: `/Users/nat/Projects/dbsp.net`

Restore dependencies once (optional but fast):

```bash
dotnet restore
```

## Running the tutorial sample

```bash
dotnet run --project examples/DBSP.Tutorials -- --sample getting-started
```

Expected output (truncated):

```
step 0 delta  -> [(alice, +1); (bob, +1)]
step 0 state  -> [(alice, 1); (bob, 1)]
step 1 delta  -> [(alice, -1); (charlie, +1)]
step 1 state  -> [(alice, 0); (bob, 1); (charlie, 1)]
...
```

Each **delta** array is intentionally tiny, expressing only what changed during
that logical step. The `IntegrateOperator` keeps a running total, so the **state**
array is always the latest per-customer order count.

## Code highlights

```fsharp
let integrate = new IntegrateOperator<string>()

let applyStep delta =
    task {
        let! snapshot = integrate.EvalAsync delta
        dump "delta" delta
        dump "state" snapshot
    }
```

- `IntegrateOperator` adds the delta to its internal accumulator and returns the
  fresh state. No full recomputation happens.
- The sample feeds a sequence of hand-written deltas to keep the example focused
  on DBSP concepts rather than data generation.

## Next steps

Continue with [Building Your First Circuit](first-circuit.md) to wire operators
into a reusable runtime, or jump to [Understanding Z-Sets](understanding-zsets.md)
to explore more set algebra patterns.
