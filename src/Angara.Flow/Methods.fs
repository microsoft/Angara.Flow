module Angara.Execution.Methods

open Angara.Graph
open Angara.StateMachine

type DataFlowGraph = DataFlowGraph<Method>

type DataFlowState = DataFlowState<Method,VertexState<MethodVertexData>>

[<Sealed; Class>]
type Function<'inp,'out>(func: 'inp -> 'out) =
    inherit Method([typeof<'inp>], [typeof<'out>])

    override x.Execute(inputs:Artefact list, _) =
        seq{
            yield [func(inputs.[0] :?> 'inp)], None
        }
    
[<Sealed; Class>]
type IterativeFunction<'inp,'out>(func: 'inp -> 'out seq) =
    inherit Method

    override x.Execute(inputs:Artefact list, _) =
        seq{
            yield [func(inputs.[0] :?> 'inp)], None
        }


[<Sealed; Class>]
type ResumableIterativeFunction<'inp,'out,'resume> =
    inherit Method
    new : ('inp * ('out * 'resume) option -> ('out * 'resume) seq)

/// FlowFunction method execution is evaluation of a certain dataflow.
[<Sealed; Class>]
type FlowFunction = 
    inherit Method
    new : DataFlowGraph * DataFlowState * (Method * InputRef) list * (Method * OutputRef) list