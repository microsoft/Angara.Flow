module Angara.Execution.Methods

open Angara.Graph
open Angara.StateMachine
open Angara.Execution

//////////////////////////////////////
//
// Supporting Types
//
//////////////////////////////////////


type DataFlowGraph = DataFlowGraph<Method>

type DataFlowState = DataFlowState<Method,VertexState<MethodVertexData>>

/////////////////////////////////////
//
// Method Implementations
//
/////////////////////////////////////

[<Sealed; Class>]
type Function<'inp,'out> =
    inherit Method
    new : ('inp -> 'out)

[<Sealed; Class>]
type IterativeFunction<'inp,'out> =
    inherit Method
    new : ('inp -> 'out seq)

[<Sealed; Class>]
type ResumableIterativeFunction<'inp,'out,'resume> =
    inherit Method
    new : ('inp * ('out * 'resume) option -> ('out * 'resume) seq)

/// FlowFunction method execution is evaluation of a certain dataflow.
[<Sealed; Class>]
type FlowFunction = 
    inherit Method
    new : DataFlowGraph * DataFlowState * (Method * InputRef) list * (Method * OutputRef) list
