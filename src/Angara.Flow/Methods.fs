module Angara.Execution.Methods

open Angara.Graph
open Angara.StateMachine
open Angara.Execution


[<Sealed; Class>]
type Function<'inp,'out>(func: 'inp -> 'out) =
    inherit Method([typeof<'inp>], [typeof<'out>])

    override x.Execute(inputs:Artefact list, _) =
        seq{
            yield Reproduce(inputs, null), None
        }

    override x.Reproduce(inputs:Artefact list, _) =
        [func(inputs.[0] :?> 'inp) |> box]
    
//[<Sealed; Class>]
//type IterativeFunction<'inp,'out>(func: 'inp -> 'out seq) =
//    inherit Method
//
//    override x.Execute(inputs:Artefact list, _) =
//        seq{
//            yield [func(inputs.[0] :?> 'inp)], None
//        }
//
//
//[<Sealed; Class>]
//type ResumableIterativeFunction<'inp,'out,'resume> =
//    inherit Method
//    new : ('inp * ('out * 'resume) option -> ('out * 'resume) seq)
//
