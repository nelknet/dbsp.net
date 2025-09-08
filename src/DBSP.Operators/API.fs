/// DBSP.Operators API - Public interface for operator implementations
namespace DBSP.Operators

/// Public API exports for DBSP operator functionality
module API =
    /// Core operator interfaces and base classes
    module Interfaces = Interfaces
    /// Linear operators (Map, Filter, FlatMap, etc.)
    module Linear = LinearOperators
    /// Join operators (InnerJoin, LeftJoin, SemiJoin, AntiJoin)
    module Join = JoinOperators  
    /// Aggregation operators (Sum, Count, Average, GroupBy)
    module Aggregate = AggregateOperators