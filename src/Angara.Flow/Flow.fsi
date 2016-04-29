namespace Angara

open System
open Angara.Graph
open Angara.StateMachine
open Angara.Execution

[<Interface>]
type IFlow<'v,'d when 'v:comparison and 'v:>IVertex and 'd:>IVertexData> =
    inherit IDisposable
    inherit IEngine<'v,'d>

    /// Allows to get a current state of computations as an instance of Angara.Work. 
    /// Returned object is immutable and doesn't change as computations go further.
    //abstract member GetState : unit -> Async<Angara.Work>

    /// Awaits until the work is complete or incomplete, i.e. flow has no running methods.
    /// State of a stable flow doesn't change.
    /// If a flow is complete, the returned list is empty; otherwise, it describes which methods couldn't be complete.
    //abstract member Await : predicate : (State -> bool) -> Async<(MethodVertex * State.VectorIndex * State.IncompleteReason) list>