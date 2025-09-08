/// BenchmarkDotNet performance tests for DBSP operators
/// Measuring async evaluation overhead, state management, and incremental computation performance
module DBSP.Tests.Performance.OperatorBenchmarks

open BenchmarkDotNet.Attributes
open BenchmarkDotNet.Running
open System.Threading.Tasks
open DBSP.Core.ZSet
open DBSP.Core.IndexedZSet
open DBSP.Operators.Interfaces
open DBSP.Operators.LinearOperators
open DBSP.Operators.JoinOperators
open DBSP.Operators.AggregateOperators

[<MemoryDiagnoser>]
[<SimpleJob>]
type LinearOperatorBenchmarks() =
    
    [<Params(100, 1_000, 10_000)>]
    member val SetSize = 0 with get, set
    
    member val private testZSet: ZSet<int> = ZSet.empty<int> with get, set
    member val private mapOp: IUnaryOperator<ZSet<int>, ZSet<int>> = Unchecked.defaultof<_> with get, set
    member val private filterOp: IUnaryOperator<ZSet<int>, ZSet<int>> = Unchecked.defaultof<_> with get, set
    member val private negateOp: IUnaryOperator<ZSet<int>, ZSet<int>> = Unchecked.defaultof<_> with get, set
    
    [<GlobalSetup>]
    member this.Setup() =
        let random = System.Random(42)
        let data = Array.init this.SetSize (fun i -> (i, random.Next(-10, 11)))
        this.testZSet <- ZSet.ofList (Array.toList data)
        this.mapOp <- LinearOperators.zsetMap (fun x -> x * 2)
        this.filterOp <- LinearOperators.zsetFilter (fun x -> x % 2 = 0)
        this.negateOp <- LinearOperators.negate
    
    [<Benchmark(Baseline = true)>]
    member this.ZSet_Map_Async() = task {
        let! result = this.mapOp.EvalAsync(this.testZSet)
        return result
    }
        
    [<Benchmark>]
    member this.ZSet_Filter_Async() = task {
        let! result = this.filterOp.EvalAsync(this.testZSet)
        return result
    }
        
    [<Benchmark>]
    member this.ZSet_Negate_Async() = task {
        let! result = this.negateOp.EvalAsync(this.testZSet)
        return result
    }

[<MemoryDiagnoser>]
type BinaryOperatorBenchmarks() =
    
    [<Params(100, 1_000, 10_000)>]
    member val SetSize = 0 with get, set
    
    member val private zset1: ZSet<int> = ZSet.empty<int> with get, set
    member val private zset2: ZSet<int> = ZSet.empty<int> with get, set
    member val private unionOp: IBinaryOperator<ZSet<int>, ZSet<int>, ZSet<int>> = Unchecked.defaultof<_> with get, set
    member val private minusOp: IBinaryOperator<ZSet<int>, ZSet<int>, ZSet<int>> = Unchecked.defaultof<_> with get, set
    
    [<GlobalSetup>]
    member this.Setup() =
        let random = System.Random(42)
        let data1 = Array.init this.SetSize (fun i -> (i, random.Next(-5, 6)))
        let data2 = Array.init this.SetSize (fun i -> (i + this.SetSize/2, random.Next(-5, 6)))
        this.zset1 <- ZSet.ofList (Array.toList data1)
        this.zset2 <- ZSet.ofList (Array.toList data2)
        this.unionOp <- LinearOperators.union
        this.minusOp <- LinearOperators.minus
    
    [<Benchmark>]
    member this.ZSet_Union_Async() = task {
        let! result = this.unionOp.EvalAsync this.zset1 this.zset2
        return result
    }
        
    [<Benchmark>]
    member this.ZSet_Minus_Async() = task {
        let! result = this.minusOp.EvalAsync this.zset1 this.zset2
        return result
    }

[<MemoryDiagnoser>]
type AggregationOperatorBenchmarks() =
    
    [<Params(100, 1_000, 10_000)>]
    member val SetSize = 0 with get, set
    
    member val private testData: ZSet<int * int> = ZSet.empty<int * int> with get, set
    member val private countOp: IUnaryOperator<ZSet<int * int>, ZSet<int * int64>> = Unchecked.defaultof<_> with get, set
    member val private sumOp: IUnaryOperator<ZSet<int * int>, ZSet<int * int>> = Unchecked.defaultof<_> with get, set
    member val private groupOp: IUnaryOperator<ZSet<int * string>, IndexedZSet<int, int * string>> = Unchecked.defaultof<_> with get, set
    
    [<GlobalSetup>]
    member this.Setup() =
        let random = System.Random(42)
        // Create key-value pairs with multiple keys for grouping
        let data = Array.init this.SetSize (fun i -> 
            let key = i % (this.SetSize / 10 + 1) // Group into ~10 keys
            let value = random.Next(1, 100)
            ((key, value), 1))
        this.testData <- ZSet.ofList (Array.toList data)
        this.countOp <- AggregateOperators.count
        this.sumOp <- AggregateOperators.intSum
        
        // For group by - use string data
        let stringData = Array.init this.SetSize (fun i ->
            let key = i % 10
            let value = sprintf "value_%d" i
            ((key, value), 1))
        let stringZSet = ZSet.ofList (Array.toList stringData)
        this.groupOp <- AggregateOperators.groupBy fst
    
    [<Benchmark>]
    member this.Count_Incremental() = task {
        let! result = this.countOp.EvalAsync(this.testData)
        return result
    }
        
    [<Benchmark>]
    member this.Sum_Incremental() = task {
        let! result = this.sumOp.EvalAsync(this.testData)
        return result
    }

[<MemoryDiagnoser>]
type JoinOperatorBenchmarks() =
    
    [<Params(100, 1_000, 5_000)>]  // Smaller for joins due to complexity
    member val SetSize = 0 with get, set
    
    member val private leftData: ZSet<int * string> = ZSet.empty<int * string> with get, set
    member val private rightData: ZSet<int * int> = ZSet.empty<int * int> with get, set
    member val private joinOp: IBinaryOperator<ZSet<int * string>, ZSet<int * int>, IndexedZSet<int, string * int>> = Unchecked.defaultof<_> with get, set
    member val private semiJoinOp: IBinaryOperator<ZSet<int * string>, ZSet<int * int>, ZSet<int * string>> = Unchecked.defaultof<_> with get, set
    
    [<GlobalSetup>]
    member this.Setup() =
        let random = System.Random(42)
        // Create data with overlapping keys for realistic joins
        let leftPairs = Array.init this.SetSize (fun i -> 
            let key = i % (this.SetSize / 3 + 1) // About 1/3 overlap
            let value = sprintf "left_%d" i
            ((key, value), 1))
        let rightPairs = Array.init this.SetSize (fun i ->
            let key = (i + this.SetSize / 6) % (this.SetSize / 3 + 1) // Offset overlap
            let value = random.Next(1, 1000)
            ((key, value), 1))
            
        this.leftData <- ZSet.ofList (Array.toList leftPairs)
        this.rightData <- ZSet.ofList (Array.toList rightPairs)
        this.joinOp <- JoinOperators.innerJoin
        this.semiJoinOp <- JoinOperators.semiJoin
    
    [<Benchmark>]
    member this.InnerJoin_Incremental() = task {
        let! result = this.joinOp.EvalAsync this.leftData this.rightData
        return result
    }
        
    [<Benchmark>]
    member this.SemiJoin_Incremental() = task {
        let! result = this.semiJoinOp.EvalAsync this.leftData this.rightData
        return result
    }

/// Async overhead benchmarks comparing sync vs async execution
[<MemoryDiagnoser>]
type AsyncOverheadBenchmarks() =
    
    [<Params(1_000, 10_000)>]
    member val SetSize = 0 with get, set
    
    member val private testZSet: ZSet<int> = ZSet.empty<int> with get, set
    
    [<GlobalSetup>]
    member this.Setup() =
        let data = Array.init this.SetSize (fun i -> (i, 1))
        this.testZSet <- ZSet.ofList (Array.toList data)
    
    [<Benchmark(Baseline = true)>]
    member this.Direct_ZSet_Map() =
        ZSet.mapKeys (fun x -> x * 2) this.testZSet
        
    [<Benchmark>]
    member this.Operator_ZSet_Map_Async() = task {
        let mapOp = LinearOperators.zsetMap (fun x -> x * 2)
        let! result = mapOp.EvalAsync(this.testZSet)
        return result
    }
    
    [<Benchmark>]
    member this.Direct_ZSet_Union() =
        ZSet.union this.testZSet this.testZSet
        
    [<Benchmark>]
    member this.Operator_ZSet_Union_Async() = task {
        let unionOp = LinearOperators.union
        let! result = unionOp.EvalAsync this.testZSet this.testZSet
        return result
    }