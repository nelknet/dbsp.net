/// DBSP.Core Library - Core types and modules for Database Stream Processor
namespace DBSP.Core

/// Re-export core modules for convenience
module Core =
    /// Algebraic operations and SRTP-based mathematical foundations
    module Algebra = Algebra
    /// Z-sets with multiplicities for incremental computation
    module ZSet = ZSet
    /// Indexed Z-sets for efficient joins and group operations
    module IndexedZSet = IndexedZSet  
    /// Temporal streams for time-based processing
    module Stream = Stream
