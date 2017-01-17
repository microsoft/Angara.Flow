namespace Angara.States

open Angara.Graph

module Messages =
    ////////////////////////
    //
    // State Machine Message
    //
    ////////////////////////

    type Response<'a> =
        | Success       of 'a
        | Exception     of System.Exception
    type ReplyChannel<'a> = Response<'a> -> unit

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
          
    /// A message that contains results for the next iteration of method execution.
    type IterationMessage<'v> =
        { Vertex: 'v
          Index: VertexIndex          
          Result: IVertexData
          StartTime: TimeIndex }

    /// A message notifying the method execution is successfully completed and the last iteration was final.
    type SucceededMessage<'v> =
        { Vertex: 'v
          Index: VertexIndex          
          StartTime: TimeIndex }


    /// A message for the state machine indicating that vertex evaluation
    /// for the given index has failed.
    type FailedMessage<'v> =
        { Vertex: 'v
          Index: VertexIndex
          Failure: exn
          StartTime: TimeIndex }

    type StopMessage<'v> =
        { Vertex: 'v
          Index: VertexIndex }

    /// Represents messages processed by the `StateMachine`.
    type Message<'v when 'v:comparison and 'v:>IVertex> =
        /// Starts methods. Replies true, if method is started; otherwise, false.
        | Start         of StartMessage<'v> //* ReplyChannel<bool>
        | Stop          of StopMessage<'v>
        | Iteration     of IterationMessage<'v>
        | Succeeded     of SucceededMessage<'v> // todo: hot/cold final artefacts (incl. distributed case, June/July 2016)
        | Failed        of FailedMessage<'v>

    
    /// Produces new state and state changes for the given state and a message.
    val transition : Message<'v> -> State<'v> -> StateUpdate<'v>


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
//val normalize<'v when 'v:comparison and 'v:>IVertex> : DataFlowGraph<'v> * DataFlowState<'v,VertexState> -> DataFlowState<'v,VertexState> * Changes<'v>
    
[<Sealed>]
type StateMachine<'v when 'v:comparison and 'v:>IVertex> = 
    interface System.IDisposable
    member State : State<'v>
    member Start : unit -> unit    
    member Changes : System.IObservable<StateUpdate<'v>>

    /// Creates new state machine from the given initial state. 
    /// The state machine will read messages from the `source` which must be empty unless the state machine is started.
    /// To start the state machine, so that it will start reading and handling the messages, call `Start` method.
    /// The `matchOutput` function returns true, if both data objects contain the given output and the both outputs are equal.
    static member CreateSuspended : source:System.IObservable<Messages.Message<'v>> -> initialState:State<'v> -> StateMachine<'v>
