namespace Angara   
open Angara.Graph

module StateMachine =
    ////////////////////////
    //
    // Vertex Status
    //
    ////////////////////////

    /// Allows to represent a local linear time of the state machine.
    /// Each state transition happens at a different time index;
    /// the latter transition time index is greater than earlier transition time index.
    type TimeIndex = uint64

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
        | Complete                  of iteration: int    
        /// The vertex execution has successfully completed after the given number of iterations,
        /// but the output is missing in the dataflow state,
        /// so the vertex is being executed again to get the execution output.
        /// The `startTime` contains the state machine time index when the vertex entered this status.
        | CompleteStarted           of iteration: int * startTime: TimeIndex
        /// The vertex execution has successfully completed but the output is missing in the dataflow state,
        /// but the output is missing in the dataflow state,
        /// so the vertex is required to be executed again, but it cannot while there is an input vertex
        /// which also has no state and must be executed.
        | CompleteStartRequested    of iteration: int
        /// The vertex is being executed, has completed given number of iterations and entered this status at the given `startTime`.
        | Started                   of iteration: int * startTime: TimeIndex
        /// The vertex cannot be executed and was not successfully completed previously.
        | Incomplete                of reason: IncompleteReason
        /// Can be executed.
        /// `time` keeps time index when the vertex entered this status.
        | CanStart                  of time: TimeIndex    
        member IsUpToDate : bool
        member IsCompleted : bool
        member IsIncompleteReason : IncompleteReason -> bool
        member TryGetIterationCount : int option

    /// Keeps status and data of a vertex as part of the StateMachine state.
    type VertexState<'d>  = {
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
        /// Creates a `VertexState` instance indicating a vertex that successfully completed its evaluation.
        static member Complete : 'd -> VertexState<'d>

    /// An immutable type which keeps state for the StateMachine.
    type State<'v,'d when 'v:comparison and 'v:>IVertex> =
        { Graph : DataFlowGraph<'v>
          FlowState : DataFlowState<'v, VertexState<'d>> 
          TimeIndex : TimeIndex }    

    ////////////////////////
    //
    // State Changes
    //
    ////////////////////////

    /// Describes changes that can occur with dataflow vertex.
    type VertexChanges<'d> = 
        | New of MdVertexState<VertexState<'d>>
        | Removed 
        /// Shape of a vertex state probably is changed.
        | ShapeChanged of old:MdVertexState<VertexState<'d>> * current:MdVertexState<VertexState<'d>> * isConnectionChanged:bool 
        /// Some items of a vertex state are changed.
        | Modified of indices:Set<VertexIndex> * old:MdVertexState<VertexState<'d>> * current:MdVertexState<VertexState<'d>> * isConnectionChanged:bool 
    
    type Changes<'v,'d when 'v : comparison> = Map<'v, VertexChanges<'d>>

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

    /// A message for the state machine which describes a batch of operations
    /// to be performed by the state machine atomically in the certain order:
    /// disconnect vertices; remove vertices; merge graph; connect vertices.
    type AlterMessage<'v,'d when 'v:comparison and 'v:>IVertex> =
        { Disconnect:  (('v * InputRef) * ('v * OutputRef) option) list 
          Remove:      ('v * RemoveStrategy) list 
          Merge:       DataFlowGraph<'v> * DataFlowState<'v,VertexState<'d>>
          Connect:     AlterConnection<'v> list }

    /// A message for the state machine which says that the vertex should be started,
    /// if it is in a proper state. 
    type StartMessage<'v> =
        { Vertex: 'v
          /// Specified which vertex slice is to be evaluated. 
          /// If index is not provided, all slices of the vertex are expected to be
          /// in "CanStart" status and all are started; otherwise, nothing happens and message is replied with "false".
          Index: VertexIndex option
          /// If given, the time when the vertex turned to the `CanStart` status must equal `CanStartTime` of the message;
          /// otherwise, the vertex will not start and the replied value is `false`.
          /// This allows to guarantee that the vertex of the observed state is started.
          CanStartTime: TimeIndex option }

    type SucceededResult<'d> = 
        | IterationResult of data:'d * iteration : int * isFinal: bool
        /// Indicates that the previous SucceededMessage was the last iteration.
        | NoMoreIterations

    /// A message for the state machine indicating that an iteration of evaluation 
    /// or the evaluation itself is successfully completes;
    /// includes the results of the evaluation.
    type SucceededMessage<'v,'d> =
        { Vertex: 'v
          Index: VertexIndex          
          Result: SucceededResult<'d>
          StartTime: TimeIndex }

    /// A message for the state machine indicating that vertex evaluation
    /// for the given index has failed.
    type FailedMessage<'v> =
        { Vertex: 'v
          Index: VertexIndex
          Failure: exn
          StartTime: TimeIndex }


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

    /// Represents messages processed by the `StateMachine`.
    type Message<'v,'d when 'v:comparison and 'v:>IVertex> =
        | Alter         of AlterMessage<'v,'d> * reply: ReplyChannel<unit>
        /// Starts methods. Replies true, if method is started; otherwise, false.
        | Start         of StartMessage<'v> * ReplyChannel<bool>
        | Stop          of 'v
        | Succeeded     of SucceededMessage<'v,'d>
        | Failed        of FailedMessage<'v>
        /// Todo: will be combined with `Succeeded`
        | RemoteSucceeded of RemoteVertexSucceeded<'v> 

    ////////////////////////
    //
    // State Machine
    //
    ////////////////////////

    /// Allows to get certain information about vertex outputs, 
    /// which is necessary for the state machine.
    [<Interface>]
    type IVertexData =
        /// Returns a value indicating whether the data is available; if not, it should be re-evaluated.
        abstract member Contains : OutputRef -> bool
        /// If the data contains the given output and the output is a one-dimensional array,
        /// returns length of the array; otherwise, returns `None`.
        abstract member TryGetShape : OutputRef -> int option

    /// Returns a new state such that each of the graph vertices has a correct state.
    /// The given vertex state is used as-is but is checked for correctness;
    /// if it has missing states, they are filled with a state based on the dependency graph.
    /// The returned graph and time index are identical to the original graph and time index.
    /// The returned changes describe the changes made in the returned state if compare to the given state.
    val normalize<'v,'d when 'v:comparison and 'v:>IVertex and 'd:>IVertexData> : DataFlowGraph<'v> * DataFlowState<'v,VertexState<'d>> -> DataFlowState<'v,VertexState<'d>> * Changes<'v,'d>
    

    /// Awaits until the state becomes final,
    /// i.e. there are no methods that are being evaluated or can be evaluated in the flow.
    /// Returns the found final flow state.
    val awaitFinal : System.IObservable<State<'v,'d> * Changes<'v,'d>> -> Async<State<'v,'d>>

    type StateStatus =
        | Complete 
        | Incomplete
        | NonFinal

    val status : DataFlowState<'v,VertexState<'d>> -> StateStatus

open StateMachine

[<Sealed>]
type StateMachine<'v,'d when 'v:comparison and 'v:>IVertex> = 
    interface System.IDisposable
    member State : State<'v,'d>
    member Start : unit -> unit    
    member Changes : System.IObservable<State<'v,'d> * Changes<'v,'d>>
    
    /// Alters the flow dependency graph by performing the batch of operations in the specific order:
    /// disconnect vertices; remove vertices; merge with another graph; connect vertices.
    /// The operation asynchronously completes when all the operations succeed.
    member AlterAsync : ((('v*InputRef) * ('v*OutputRef) option) list * ('v*RemoveStrategy) list * (DataFlowGraph<'v>*DataFlowState<'v,VertexState<'d>>) * AlterConnection<'v> list) -> Async<unit>

    /// Creates new state machine from the given initial state. 
    /// The state machine will read messages from the `source` which must be empty unless the state machine is started.
    /// To start the state machine, so that it will start reading and handling the messages, call `Start` method.
    /// The `matchOutput` function returns true, if both data objects contain the given output and the both outputs are equal.
    static member CreateSuspended : source:System.IObservable<Message<'v, 'd>> -> initialState:(DataFlowGraph<'v> * DataFlowState<'v,VertexState<'d>>) -> matchOutput:('d->'d->OutputRef->bool) -> StateMachine<'v,'d>
