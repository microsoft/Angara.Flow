module Angara.StateMachine

val initial<'v, 'vs when 'v : comparison and 'v :> IVertex and 'vs :> IVertexData> : 
    DataFlowState<'v,'vs> * Changes 

/// Makes a single execution step by evolving a given work in response to a message.
val transition<'v, 'vs when 'v : comparison and 'v :> IVertex and 'vs :> IVertexData> :
    TimeIndex -> DataFlowGraph<'v> * DataFlowState<'v, 'vs> -> Message<'v> -> DataFlowGraph<'v> * DataFlowState<'v, 'vs> * Changes * DeferredResponse


type TimeIndex = uint32

type Message<'vertex when 'vertex : comparison and 'vertex :> IWorkVertex> =
    | Alter         of disconnect:  (('vertex * InputRef) * ('vertex * OutputRef) option) list *
                       remove:      AlterRemoveVertex<'vertex> list *
                       merge:       (WorkGraph<'vertex>*WorkState<'vertex>) *
                       connect:     AlterConnection<'vertex> list *
                       reply:       ReplyChannel<unit>
    /// Starts methods. Replies true, if method is started; otherwise, false.
    /// If index is not provided for a vector method, all slices of that methods are expected to be
    /// in "CanStart" status and all are started; otherwise, nothing happens and message is replied with "false".
    | Start         of 'vertex * Option<VectorIndex * Option<TimeIndex>> * ReplyChannel<bool>
    | Stop          of 'vertex
    | Succeeded     of 'vertex * VectorIndex * final: bool * List<Artefact> * startTime: TimeIndex // if not "final" the next iterations are coming
    | Failed        of 'vertex * VectorIndex * failure: System.Exception * startTime: TimeIndex
    | RemoteSucceeded of RemoteVertexSucceeded<'vertex> 
    
    

/////////////////////////////////////
//
// State Machine
//
/////////////////////////////////////

[<Interface>]
type IStateMachine<'state, 'changes> = 
    inherit System.IDisposable
    abstract OnChanged : System.IObservable<'state * 'changes>
    abstract Start : unit -> unit
    abstract Last: 'state

