/// Public API for DBSP Circuit library providing clean interfaces for circuit construction, 
/// execution, and optimization with high-performance Task-based evaluation and async I/O
namespace DBSP.Circuit.API

/// Re-export circuit construction functionality
module Builder = DBSP.Circuit.CircuitBuilderModule

/// Re-export circuit runtime execution functionality
module Runtime = DBSP.Circuit.CircuitRuntimeModule

/// Re-export dependency-aware scheduling functionality
module Scheduler = DBSP.Circuit.SchedulerModule

/// Re-export circuit optimization functionality
module Optimizer = DBSP.Circuit.CircuitOptimizerModule

/// Re-export async input/output handle functionality
module Handles = DBSP.Circuit.HandleFactoryModule

/// Re-export circuit visualization and debugging functionality
module Visualization = DBSP.Circuit.CircuitVisualization