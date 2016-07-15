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

    /// Each of the elements of this array corresponds to the method output with same index
    /// and if the output item is a 1d-array, it keeps number of elements in that array; 
    /// otherwise, it is zero.
    type OutputShape = int list

    /// Indicates how the state machine considers a vertex item.
    type VertexStatus =
        /// The vertex execution has successfully completed all iterations.
        | Final of OutputShape       
        | Final_MissingInputOnly of OutputShape    
        | Final_MissingOutputOnly of OutputShape
        | Final_MissingInputOutput of OutputShape                         
        /// The input methods are "final", "paused" or "continues"; 
        /// and the method isn't being executed but previously produced the output at least one time.
        | Paused of OutputShape 
        | Paused_MissingOutputOnly of OutputShape
        | Paused_MissingInputOnly of OutputShape
        | Paused_MissingInputOutput of OutputShape
        /// Can be executed.
        /// `time` keeps time index when the vertex entered this status.
        | CanStart                  of time: TimeIndex 
        /// The vertex is being executed and hasn't produce its output yet,
        /// so the dependent vertices cannot start.
        /// It entered this status at the given `startTime`.
        | Started                   of startTime: TimeIndex      
        /// Method is being executed and it is already produced first output.
        | Continues                 of startTime: TimeIndex      
        /// The vertex execution has successfully completed,
        /// but the output is missing in the dataflow state,
        /// so the vertex is being executed again to reproduce the execution output.
        /// The `startTime` contains the state machine time index when the vertex entered this status.
        | Reproduces                of startTime: TimeIndex * outputShape : OutputShape
        /// The vertex execution has successfully completed but the output is missing in the dataflow state,
        /// so the vertex is required to be executed again, but it cannot while there is an input vertex
        /// which also has no state and must be executed.
        | ReproduceRequested of OutputShape   
        /// The vertex cannot be executed and was not successfully completed previously.
        | Incomplete                of reason: IncompleteReason
   
        member IsUpToDate : bool
        member IsIncompleteReason : IncompleteReason -> bool
        member TryGetShape : unit -> OutputShape option

    /// Keeps status and data of a vertex as part of the StateMachine state.
    [<AbstractClass>]
    type VertexState = 
        /// Keeps status of the vertex in terms of the state machine.
        member Status : VertexStatus
        /// Returns a clone of the vertex state which has the given status.
        member WithStatus : VertexStatus -> VertexState
    // How to create new? - real VertexState type is generic type parameter which has a constructor

    /// An immutable type which keeps state for the StateMachine.
    type State<'v when 'v:comparison and 'v:>IVertex> =
        { Graph : DataFlowGraph<'v>
          FlowState : DataFlowState<'v, VertexState> 
          TimeIndex : TimeIndex }    

    ////////////////////////
    //
    // State Changes
    //
    ////////////////////////

    /// Describes changes that can occur with dataflow vertex.
    type VertexChanges = 
        /// New vertex is added.
        | New of MdVertexState<VertexState>
        /// Vertex is removed.
        | Removed 
        /// Shape of a vertex state probably is changed.
        | ShapeChanged of old:MdVertexState<VertexState> * current:MdVertexState<VertexState> * isConnectionChanged:bool 
        /// Some items of a vertex state are changed.
        | Modified of indices:Set<VertexIndex> * old:MdVertexState<VertexState> * current:MdVertexState<VertexState> * isConnectionChanged:bool 
    
    type Changes<'v when 'v : comparison> = Map<'v, VertexChanges>

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
    type AlterMessage<'v when 'v:comparison and 'v:>IVertex> =
        { Disconnect:  (('v * InputRef) * ('v * OutputRef) option) list 
          Remove:      ('v * RemoveStrategy) list 
          Merge:       DataFlowGraph<'v> * DataFlowState<'v,VertexState>
          Connect:     AlterConnection<'v> list }

    /// A message for the state machine which says that the vertex should be started,
    /// if it is in a proper state. 
    type StartMessage<'v> =
        { Vertex: 'v
          /// Specified which vertex slice is to be evaluated. 
          Index: VertexIndex
          /// If given, the time when the vertex turned to the `CanStart` status must equal `CanStartTime` of the message;
          /// otherwise, the vertex will not start and the replied value is `false`.
          /// This allows to guarantee that the vertex of the observed state is started.
          CanStartTime: TimeIndex option }

    type SucceededResult<'d> = 
        /// Contains results of the iteration with the given number.
        | IterationResult of result:'d
        /// Indicates that the previous SucceededMessage was the last iteration, though its `isFinal` was `false`.
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
    
open StateMachine

[<Sealed>]
type StateMachine<'v,'d when 'v:comparison and 'v:>IVertex and 'd:>IVertexData> = 
    interface System.IDisposable
    member State : State<'v,'d>
    member Start : unit -> unit    
    member Changes : System.IObservable<State<'v,'d> * Changes<'v,'d>>

    /// Creates new state machine from the given initial state. 
    /// The state machine will read messages from the `source` which must be empty unless the state machine is started.
    /// To start the state machine, so that it will start reading and handling the messages, call `Start` method.
    /// The `matchOutput` function returns true, if both data objects contain the given output and the both outputs are equal.
    static member CreateSuspended : source:System.IObservable<Message<'v, 'd>> -> matchOutput:('d->'d->OutputRef->bool) -> initialState:(DataFlowGraph<'v> * DataFlowState<'v,VertexState<'d>>) -> StateMachine<'v,'d>
