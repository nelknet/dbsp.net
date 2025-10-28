module DBSP.Tests.Performance.RealisticWorkloads

open System
open BenchmarkDotNet.Attributes
open BenchmarkDotNet.Diagnosers
open DBSP.Core
open DBSP.Operators
open DBSP.Circuit

/// Streaming aggregation workload similar to Feldera's TikTok recommendation system demo
[<MemoryDiagnoser>]
[<ThreadingDiagnoser>]
type StreamingAggregationWorkload() =
    
    // Simulates user interactions: (userId, videoId, interactionType, timestamp)
    type UserInteraction = int * int * string * int64
    
    let mutable interactions: UserInteraction array = [||]
    let mutable updates: UserInteraction array = [||]
    
    [<Params(10000, 50000, 100000)>]
    member val BaseSize = 0 with get, set
    
    [<Params(100, 500, 1000)>]
    member val UpdateBatchSize = 0 with get, set
    
    [<GlobalSetup>]
    member this.Setup() =
        let random = Random(42)
        
        // Generate base interactions
        interactions <- 
            [| for i in 1..this.BaseSize do
                let userId = random.Next(1, 10000)
                let videoId = random.Next(1, 5000)
                let interactionType = 
                    match random.Next(0, 3) with
                    | 0 -> "view" | 1 -> "like" | _ -> "share"
                let timestamp = int64 i
                yield (userId, videoId, interactionType, timestamp) |]
        
        // Generate update batch
        updates <-
            [| for i in 1..this.UpdateBatchSize do
                let userId = random.Next(1, 10000)
                let videoId = random.Next(1, 5000)
                let interactionType = 
                    match random.Next(0, 3) with
                    | 0 -> "view" | 1 -> "like" | _ -> "share"
                let timestamp = int64 (this.BaseSize + i)
                yield (userId, videoId, interactionType, timestamp) |]
    
    [<Benchmark(Baseline = true)>]
    member this.NaiveAggregation() =
        // Recalculate everything from scratch
        let allData = Array.append interactions updates
        
        // Group by video and count interactions
        allData
        |> Array.groupBy (fun (_, videoId, _, _) -> videoId)
        |> Array.map (fun (videoId, interactions) ->
            let counts = 
                interactions
                |> Array.groupBy (fun (_, _, interactionType, _) -> interactionType)
                |> Array.map (fun (typ, items) -> (typ, Array.length items))
                |> Map.ofArray
            (videoId, counts))
        |> Array.length
    
    [<Benchmark>]
    member this.DBSPIncrementalAggregation() =
        // Convert to ZSets
        let baseZSet = 
            interactions 
            |> Array.map (fun item -> (item, 1))
            |> ZSet.ofSeq
        
        let updateZSet =
            updates
            |> Array.map (fun item -> (item, 1))
            |> ZSet.ofSeq
        
        // Apply incremental update
        let combined = ZSet.add baseZSet updateZSet
        
        // Group and aggregate (simplified - real DBSP would maintain incremental state)
        combined
        |> ZSet.toSeq
        |> Seq.groupBy (fun ((_, videoId, _, _), _) -> videoId)
        |> Seq.map (fun (videoId, items) ->
            let counts = 
                items
                |> Seq.groupBy (fun ((_, _, interactionType, _), _) -> interactionType)
                |> Seq.map (fun (typ, items) -> 
                    (typ, items |> Seq.sumBy (fun (_, weight) -> weight)))
                |> Map.ofSeq
            (videoId, counts))
        |> Seq.length

/// CDC (Change Data Capture) workload similar to Feldera's Debezium MySQL demo
[<MemoryDiagnoser>]
type CDCWorkload() =
    
    // Simulates database records: (id, value, version)
    type Record = int * string * int
    
    let mutable records: Map<int, Record> = Map.empty
    let mutable changeLog: (Record * int) list = [] // (record, weight) where -1 = delete, 1 = insert
    
    [<Params(5000, 10000, 20000)>]
    member val RecordCount = 0 with get, set
    
    [<Params(50, 100, 200)>]
    member val ChangesPerBatch = 0 with get, set
    
    [<GlobalSetup>]
    member this.Setup() =
        let random = Random(42)
        
        // Initialize records
        for i in 1..this.RecordCount do
            records <- records.Add(i, (i, $"value_{i}", 1))
        
        // Generate CDC change log (mix of updates and deletes)
        changeLog <- 
            [ for _ in 1..this.ChangesPerBatch do
                let id = random.Next(1, this.RecordCount + 1)
                match records.TryFind(id) with
                | Some (id, value, version) ->
                    if random.Next(0, 10) < 2 then
                        // Delete (20% chance)
                        yield ((id, value, version), -1)
                    else
                        // Update (80% chance)
                        let oldRecord = (id, value, version)
                        let newRecord = (id, $"updated_{id}", version + 1)
                        yield (oldRecord, -1)  // Delete old version
                        yield (newRecord, 1)   // Insert new version
                | None -> () ]
    
    [<Benchmark(Baseline = true)>]
    member this.NaiveCDCProcessing() =
        // Apply all changes to rebuild state
        let mutable state = records
        for (record, weight) in changeLog do
            let (id, _, _) = record
            if weight = -1 then
                state <- state.Remove(id)
            else
                state <- state.Add(id, record)
        state.Count
    
    [<Benchmark>]
    member this.DBSPCDCProcessing() =
        // Convert to ZSet representation
        let baseZSet = 
            records
            |> Map.toSeq
            |> Seq.map (fun (_, record) -> (record, 1))
            |> ZSet.ofSeq
        
        let deltaZSet = ZSet.ofList changeLog
        
        // Apply incremental CDC updates
        let result = ZSet.add baseZSet deltaZSet
        
        // Count non-zero weight entries
        result
        |> ZSet.toSeq
        |> Seq.filter (fun (_, weight) -> weight <> 0)
        |> Seq.length

/// Complex analytics pipeline similar to Feldera's e-commerce SQL demo
[<MemoryDiagnoser>]
type AnalyticsPipelineWorkload() =
    
    type Order = { OrderId: int; CustomerId: int; ProductId: int; Quantity: int; Price: decimal }
    type Customer = { CustomerId: int; Name: string; Country: string }
    type Product = { ProductId: int; Name: string; Category: string }
    
    let mutable orders: Order array = [||]
    let mutable customers: Customer array = [||]
    let mutable products: Product array = [||]
    let mutable newOrders: Order array = [||]
    
    [<Params(1000, 5000, 10000)>]
    member val OrderCount = 0 with get, set
    
    [<GlobalSetup>]
    member this.Setup() =
        let random = Random(42)
        
        // Generate customers
        customers <- 
            [| for i in 1..100 ->
                { CustomerId = i
                  Name = $"Customer_{i}"
                  Country = [|"US"; "UK"; "DE"; "FR"; "JP"|].[random.Next(0, 5)] } |]
        
        // Generate products
        products <-
            [| for i in 1..50 ->
                { ProductId = i
                  Name = $"Product_{i}"
                  Category = [|"Electronics"; "Books"; "Clothing"; "Food"|].[random.Next(0, 4)] } |]
        
        // Generate orders
        orders <-
            [| for i in 1..this.OrderCount ->
                { OrderId = i
                  CustomerId = random.Next(1, 101)
                  ProductId = random.Next(1, 51)
                  Quantity = random.Next(1, 10)
                  Price = decimal(random.Next(10, 1000)) } |]
        
        // Generate new orders
        newOrders <-
            [| for i in (this.OrderCount + 1)..(this.OrderCount + 100) ->
                { OrderId = i
                  CustomerId = random.Next(1, 101)
                  ProductId = random.Next(1, 51)
                  Quantity = random.Next(1, 10)
                  Price = decimal(random.Next(10, 1000)) } |]
    
    [<Benchmark(Baseline = true)>]
    member this.NaiveAnalyticsPipeline() =
        // Combine all orders
        let allOrders = Array.append orders newOrders
        
        // Join with customers and products, then aggregate by country and category
        let results =
            allOrders
            |> Array.choose (fun order ->
                let customer = customers |> Array.tryFind (fun c -> c.CustomerId = order.CustomerId)
                let product = products |> Array.tryFind (fun p -> p.ProductId = order.ProductId)
                match customer, product with
                | Some c, Some p -> 
                    Some (c.Country, p.Category, order.Quantity * int order.Price)
                | _ -> None)
            |> Array.groupBy (fun (country, category, _) -> (country, category))
            |> Array.map (fun ((country, category), items) ->
                let total = items |> Array.sumBy (fun (_, _, amount) -> amount)
                (country, category, total))
        
        results.Length
    
    [<Benchmark>]
    member this.DBSPAnalyticsPipeline() =
        // Convert to ZSets
        let orderZSet = 
            orders |> Array.map (fun o -> (o, 1)) |> ZSet.ofSeq
        let newOrderZSet = 
            newOrders |> Array.map (fun o -> (o, 1)) |> ZSet.ofSeq
        let customerZSet = 
            customers |> Array.map (fun c -> ((c.CustomerId, c), 1)) |> ZSet.ofSeq
        let productZSet = 
            products |> Array.map (fun p -> ((p.ProductId, p), 1)) |> ZSet.ofSeq
        
        // Apply incremental update
        let allOrdersZSet = ZSet.add orderZSet newOrderZSet
        
        // Perform joins (simplified - real DBSP would use indexed Z-sets)
        let joined =
            allOrdersZSet
            |> ZSet.toSeq
            |> Seq.choose (fun (order, orderWeight) ->
                if orderWeight = 0 then None
                else
                    let customerOpt = 
                        customerZSet
                        |> ZSet.toSeq
                        |> Seq.tryFind (fun ((cId, _), _) -> cId = order.CustomerId)
                    let productOpt = 
                        productZSet
                        |> ZSet.toSeq
                        |> Seq.tryFind (fun ((pId, _), _) -> pId = order.ProductId)
                    match customerOpt, productOpt with
                    | Some ((_, customer), _), Some ((_, product), _) ->
                        Some ((customer.Country, product.Category), order.Quantity * int order.Price)
                    | _ -> None)
            |> Seq.groupBy fst
            |> Seq.map (fun (key, items) ->
                let total = items |> Seq.sumBy snd
                (key, total))
            |> Seq.length
        
        joined
