/// DBSP.Core API - Public interface for core types and modules
namespace DBSP.Core

/// Public API exports for DBSP core functionality
module API =
    /// Algebraic operations and SRTP-based mathematical foundations
    module Algebra = Algebra
    /// Z-sets with multiplicities for incremental computation
    module ZSet = ZSet
    /// Indexed Z-sets for efficient joins and group operations
    module IndexedZSet = IndexedZSet  
    /// Temporal streams for time-based processing
    module Stream = Stream