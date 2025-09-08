/// DBSP.Operators Library - Operator implementations for Database Stream Processor
namespace DBSP.Operators

/// Re-export operator modules for convenience
module Operators =
    /// Core operator interfaces and base classes
    module Interfaces = Interfaces
    /// Linear operators (Map, Filter, FlatMap, etc.)
    module Linear = LinearOperators
    /// Join operators (InnerJoin, LeftJoin, SemiJoin, AntiJoin)
    module Join = JoinOperators  
    /// Aggregation operators (Sum, Count, Average, GroupBy)
    module Aggregate = AggregateOperators
