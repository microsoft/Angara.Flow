module Angara.StateMachine

open Angara.Graph
open Angara.State

////////////////////////
//
// Vertex Status
//
////////////////////////

/// Allows to represent a local linear time of the state machine.
/// Each state transition happens at a different time index;
/// the latter transition time index is greater than earlier transition time index.
type TimeIndex = uint32

/// A reason why a vertex cannot be executed.
type IncompleteReason = 
    /// One or more arguments do not have incoming connections.
    | UnassignedInputs
    /// One or more arguments await their values to be produced by upstream methods.
    | OutdatedInputs
    /// An exception is thrown during the last method execution.
    | ExecutionFailed   of failure: System.Exception
    /// A user requested interruption of the last method execution.
    | Stopped
    /// An upstream method completed but its output is missing in the dataflow state.
    /// This may happen when loading the method state from a storage or when the upstream vertex is executed on a remote node.
    | TransientInputs

/// Indicates how the state machine considers a vertex item.
type VertexStatus =
    /// The vertex execution has successfully completed after the given number of iterations.
    /// If `iteration` is `None`, the execution was not iterative.
    | Complete                  of iteration: int option    
    /// The vertex execution has successfully completed after the given number of iterations,
    /// but the output is missing in the dataflow state,
    /// so the vertex is being executed again to get the execution output.
    /// The `startTime` contains the state machine time index when the vertex entered this status.
    | CompleteStarted           of iteration: int option * startTime: TimeIndex
    /// The vertex execution has successfully completed but the output is missing in the dataflow state,
    /// but the output is missing in the dataflow state,
    /// so the vertex is required to be executed again, but it cannot while there is an input vertex
    /// which also has no state and must be executed.
    | CompleteStartRequested    of iteration: int option
    /// The vertex is being executed, has completed given number of iterations and entered this status at the given `startTime`.
    | Started                   of iteration: int * startTime: TimeIndex
    /// The vertex cannot be executed and was not successfully completed previously.
    | Incomplete                of reason: IncompleteReason
    /// Can be executed.
    /// `time` keeps time index when the vertex entered this status.
    | CanStart                  of time: TimeIndex    

/// Represents vertex output stored for each of the vertices as a part of the state machine state.
[<Interface>]
type IVertexData =
    /// Returns a value indicating whether the data is available; if not, it should be re-evaluated.
    abstract member Contains : OutputRef -> bool
    /// If the data contains the given output and the output is a one-dimensional array,
    /// returns length of the array; otherwise, returns `None`.
    abstract member TryGetShape : OutputRef -> int option

/// Allows to check if the output artefact matches the inner state of the object implementing this interface.
[<Interface>]
type IOutputsMatch<'d when 'd :> IVertexData> = 
    /// Returns true, if both this object and the given vertex data contain the given output
    /// and the both outputs are equal.
    abstract Match : 'd -> OutputRef -> bool

/// Keeps status and data of a vertex as part of the StateMachine state.
type VertexState<'d when 'd:>IVertexData>  = {
    /// Keeps status of the vertex in terms of the state machine.
    Status : VertexStatus
    /// Keeps data associated with the vertex.
    /// Data can be presented or missing for any status.
    /// If status is `Complete`, data still can be missing or partial. For example,
    /// the state is deserialized and data is transient.
    /// Also, if status is `Incomplete`, data can be presented but obsolete.
    Data : 'd option
} with
    /// Returns a `VertexState` instance indicating a vertex that has at least one input unassigned.
    static member Unassigned : VertexState<'d>
    /// Creates a `VertexState` instance indicating a vertex awaiting upward computation to produce one or more arguments,
    /// before it could be able to start.
    static member Outdated : VertexState<'d>

/// An immutable type which keeps state for the StateMachine.
type State<'v,'d when 'v:comparison and 'v:>IVertex and 'd:>IVertexData> =
    { Graph : DataFlowGraph<'v>
      Status : DataFlowState<'v,VertexState<'d>> 
      TimeIndex : TimeIndex }

////////////////////////
//
// State Changes
//
////////////////////////

/// Describes changes that can occur with dataflow vertex.
type VertexChanges<'d when 'd:>IVertexData> = 
    | New of MdVertexState<VertexState<'d>>
    | Removed 
    /// Shape of a vertex state probably is changed.
    | ShapeChanged of old:MdVertexState<VertexState<'d>> * current:MdVertexState<VertexState<'d>> * isConnectionChanged:bool 
    /// Some items of a vertex state are changed.
    | Modified of indices:Set<VertexIndex> * old:MdVertexState<VertexState<'d>> * current:MdVertexState<VertexState<'d>> * isConnectionChanged:bool 
    
type Changes<'v,'d when 'v : comparison and 'd:>IVertexData> = Map<'v, VertexChanges<'d>>

////////////////////////
//
// State Machine Message
//
////////////////////////

type Response<'a> =
    | Success       of 'a
    | Exception     of System.Exception
type ReplyChannel<'a> = Response<'a> -> unit

type AlterConnection<'v> =
    | ItemToItem    of target: ('v * InputRef) * source: ('v * OutputRef)
    | ItemToArray   of target: ('v * InputRef) * source: ('v * OutputRef)

type RemoveStrategy = FailIfHasDependencies | DontRemoveIfHasDependencies

/// Determines how the local proxy of a remote vertex gets the output.
type RemoteVertexExecution = 
    /// The vertex output can be fetched from a remote node.
    | Fetch 
    /// The vertex output cannot be fetched and should be computed locally.
    | Compute

type SucceededRemoteVertexItem<'v> = 
    { Vertex: 'v 
      Slice: VertexIndex
      OutputShape: int list 
      CompletionTime: TimeIndex
      Execution: RemoteVertexExecution }

type RemoteVertexSucceeded<'v> = 'v * SucceededRemoteVertexItem<'v> list

type Message<'v,'d when 'v:comparison and 'v:>IVertex and 'd:>IVertexData> =
    | Alter of 
        disconnect:  (('v * InputRef) * ('v * OutputRef) option) list *
        remove:      ('v * RemoveStrategy) list *
        merge:       (DataFlowGraph<'v> * DataFlowState<'v,VertexState<'d>>) *
        connect:     AlterConnection<'v> list *
        reply:       ReplyChannel<unit>
    /// Starts methods. Replies true, if method is started; otherwise, false.
    /// If index is not provided for a vector method, all slices of that methods are expected to be
    /// in "CanStart" status and all are started; otherwise, nothing happens and message is replied with "false".
    | Start     of 'v * Option<VertexIndex * Option<TimeIndex>> * ReplyChannel<bool>
    | Stop      of 'v
    | Succeeded of 'v * VertexIndex * final: bool * 'd * startTime: TimeIndex // if not "final" the next iterations are coming
    | Failed    of 'v * VertexIndex * failure: System.Exception * startTime: TimeIndex
    | RemoteSucceeded of RemoteVertexSucceeded<'v> 

////////////////////////
//
// State Machine
//
////////////////////////

/// Returns a new state such that each of the graph vertices has a correct state.
/// The given vertex state is used as-is but is checked for correctness;
/// if it has missing states, they are filled with a state based on the dependency graph.
/// The returned graph and time index are identical to the original graph and time index.
/// The returned changes describe the changes made in the returned state if compare to the given state.
val normalize<'v,'d when 'v:comparison and 'v:>IVertex and 'd:>IVertexData> : DataFlowGraph<'v> * DataFlowState<'v,VertexState<'d>> -> DataFlowState<'v,VertexState<'d>> * Changes<'v,'d>
    
[<Interface>]
type IStateMachine<'v,'d when 'v:comparison and 'v:>IVertex and 'd:>IVertexData and 'd:>IOutputsMatch<'d>> = 
    inherit System.IDisposable
    abstract Start : unit -> unit
    abstract State : State<'v,'d>
    abstract OnChanged : System.IObservable<State<'v,'d> * Changes<'v,'d>>

[<Class>]
type StateMachine =
    /// Creates new state machine from the given initial state. 
    /// The state machine will read messages from the `source` which must be empty unless the state machine is started.
    /// To start the state machine, so that it will start reading and handling the messages, call `Start` method.
    static member CreatePaused<'v,'d when 'v:comparison and 'v:>IVertex and 'd:>IVertexData and 'd:>IOutputsMatch<'d>> : 
        source:System.IObservable<Message<'v, 'd>> -> initialState:(DataFlowGraph<'v> * DataFlowState<'v,VertexState<'d>>) -> IStateMachine<'v,'d>