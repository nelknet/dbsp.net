/// Core algebraic operations using F# 7+ simplified SRTP syntax
/// This module provides zero-cost abstractions for mathematical operations
/// supporting DBSP's incremental computation requirements
module DBSP.Core.Algebra

open System.Runtime.CompilerServices

/// Core algebraic operations using F# 7+ simplified SRTP syntax
/// F# 7+ allows 'T instead of ^T and direct member access
[<MethodImpl(MethodImplOptions.AggressiveInlining)>]
let inline add<'T when 'T : (static member (+) : 'T * 'T -> 'T)> x y = 
    'T.(+)(x, y)

[<MethodImpl(MethodImplOptions.AggressiveInlining)>]
let inline zero<'T when 'T : (static member Zero : 'T)> = 
    'T.Zero

[<MethodImpl(MethodImplOptions.AggressiveInlining)>]
let inline negate<'T when 'T : (static member (~-) : 'T -> 'T)> x = 
    'T.(~-)(x)

[<MethodImpl(MethodImplOptions.AggressiveInlining)>]    
let inline multiply<'T when 'T : (static member (*) : 'T * 'T -> 'T)> x y = 
    'T.(*)(x, y)

[<MethodImpl(MethodImplOptions.AggressiveInlining)>]    
let inline one<'T when 'T : (static member One : 'T)> = 
    'T.One

/// Addition with null safety for optional types
let inline addOpt<'T when 'T : (static member (+) : 'T * 'T -> 'T)> (x: 'T option) (y: 'T option) : 'T option =
    match x, y with
    | Some a, Some b -> Some (add a b)
    | Some a, None -> Some a
    | None, Some b -> Some b
    | None, None -> None

/// Subtraction using negation and addition with explicit type constraints
let inline subtract x y = 
    add x (negate y)

/// Scalar multiplication with basic numeric types
let inline scalarMultiply scalar value = scalar * value