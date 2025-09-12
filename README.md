DBSP.NET Examples Worktree
==========================

This worktree hosts the `examples` branch with a tutorial-style project demonstrating naive vs incremental computation using DBSP.NET primitives.

Project:
- examples/DBSP.Examples

How to run
----------

1) Restore and build:

   dotnet build examples/DBSP.Examples/DBSP.Examples.fsproj -c Release

2) Run the tutorial:

   dotnet run --project examples/DBSP.Examples/DBSP.Examples.fsproj -c Release

What it does
------------

- Generates a small synthetic dataset of Customers and Orders.
- Times a naive recomputation (scan all orders each step) to get per-customer order counts.
- Times an incremental approach using ZSets and an Integrate operator to maintain counts via deltas.
- Prints top customers and total timing for both approaches, with explanatory commentary.

