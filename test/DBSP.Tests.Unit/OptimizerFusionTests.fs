namespace DBSP.Tests.Unit

open Xunit
open DBSP.Core.ZSet
open DBSP.Circuit
open DBSP.Circuit.CircuitBuilderModule
open DBSP.Circuit.CircuitOptimizerModule
open DBSP.Operators.LinearOperators
open DBSP.Operators.FusedOperators

module OptimizerFusionTests =

    let zsetEqual (a: ZSet<'K>) (b: ZSet<'K>) =
        let sa = a |> ZSet.toSeq |> Seq.sortBy id |> Seq.toArray
        let sb = b |> ZSet.toSeq |> Seq.sortBy id |> Seq.toArray
        sa = sb

    [<Fact>]
    let ``FilterMap fused operator preserves semantics`` () =
        // Build sample functions
        let pred x = x % 2 = 0
        let map x = x * 10
        let filterOp = ZSetFilterOperator<int>(pred)
        let mapOp = ZSetMapOperator<int,int>(map)
        let fused = FilterMapOperator<int,int>(pred, map)

        // Sample input
        let input = ZSet.ofSeq [ for i in 1..10 -> (i, 1) ]

        // Separate pipeline
        let sep = mapOp.EvalAsync(filterOp.EvalAsync(input).Result).Result
        // Fused
        let fus = fused.EvalAsync(input).Result

        Assert.True(zsetEqual sep fus)

    [<Fact>]
    let ``Optimizer fuses ZSetFilter -> ZSetMap when enabled`` () =
        // Build a tiny circuit with Filter -> Map
        let circuit, _ =
            build (fun b ->
                let f = ZSetFilterOperator<int>(fun x -> x > 0)
                let m = ZSetMapOperator<int,int>(fun x -> x + 1)
                let nidF = b.AddOperator(f, { Name = "f"; TypeInfo = ""; Location = None })
                let nidM = b.AddOperator(m, { Name = "m"; TypeInfo = ""; Location = None })
                b.ConnectNodes(nidF, nidM)
                () )

        // Enable fusion
        let opts = { OptimizerOptions.EnableFusion = true }
        let optimized = optimizeWithOptions opts circuit

        // Expect one fewer operator than before
        Assert.True(optimized.Operators.Count = circuit.Operators.Count - 1)

    [<Fact>]
    let ``Optimizer does not fuse when disabled`` () =
        let circuit, _ =
            build (fun b ->
                let f = ZSetFilterOperator<int>(fun x -> x > 0)
                let m = ZSetMapOperator<int,int>(fun x -> x + 1)
                let nidF = b.AddOperator(f, { Name = "f"; TypeInfo = ""; Location = None })
                let nidM = b.AddOperator(m, { Name = "m"; TypeInfo = ""; Location = None })
                b.ConnectNodes(nidF, nidM)
                () )

        let opts = { OptimizerOptions.EnableFusion = false }
        let optimized = optimizeWithOptions opts circuit

        Assert.True(optimized.Operators.Count = circuit.Operators.Count)

    [<Fact>]
    let ``MapFilter fused operator preserves semantics`` () =
        let map x = x + 1
        let pred y = y % 2 = 0 // predicate on mapped value
        let mapOp = ZSetMapOperator<int,int>(map)
        let filterOnOut = ZSetFilterOperator<int>(pred)
        let fused = MapFilterOperator<int,int>(map, pred)

        let input = ZSet.ofSeq [ for i in 1..5 -> (i, 1) ]
        let sep = filterOnOut.EvalAsync(mapOp.EvalAsync(input).Result).Result
        let fus = fused.EvalAsync(input).Result
        Assert.True(zsetEqual sep fus)

    [<Fact>]
    let ``Optimizer fuses ZSetMap -> ZSetFilter when enabled`` () =
        let circuit, _ =
            build (fun b ->
                let m = ZSetMapOperator<int,int>(fun x -> x + 1)
                let f = ZSetFilterOperator<int>(fun y -> y % 2 = 0)
                let nidM = b.AddOperator(m, { Name = "m"; TypeInfo = ""; Location = None })
                let nidF = b.AddOperator(f, { Name = "f"; TypeInfo = ""; Location = None })
                b.ConnectNodes(nidM, nidF)
                () )

        let opts = { OptimizerOptions.EnableFusion = true }
        let optimized = optimizeWithOptions opts circuit
        Assert.True(optimized.Operators.Count = circuit.Operators.Count - 1)
