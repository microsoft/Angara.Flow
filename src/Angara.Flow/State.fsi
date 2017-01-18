namespace Angara.States

open Angara
open Angara.Graph

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
    | Continues                 of startTime: TimeIndex * outputShape : OutputShape      
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


/// Allows to get certain information about vertex outputs, 
/// which is necessary for the state machine.
[<Interface>]
type IVertexData =
    abstract member Shape : OutputShape
    
/// Keeps status and data of a vertex as part of the StateMachine state.
type VertexState<'d when 'd:>IVertexData> = 
    { 
      /// Keeps status of the vertex in terms of the state machine.
      Status : VertexStatus
      Data : 'd option
    } with 
    static member Unassigned : VertexState<'d>
    static member Outdated : VertexState<'d>
    static member Final : 'd -> VertexState<'d>
    /// Creates a vertex state of given rank with zero shape output. Data is None in this case.
    /// Otherwise we cannot create an instance of 'd in general.
    static member FinalEmpty : rank:int -> VertexState<'d>

/// An immutable type which keeps state for the StateMachine.
type State<'v,'d when 'v:comparison and 'v:>IVertex and 'd:>IVertexData> =
    { Graph : DataFlowGraph<'v>
      FlowState : DataFlowState<'v, VertexState<'d>> 
      TimeIndex : TimeIndex }


////////////////////////
//
// State Changes
//
////////////////////////

/// Describes changes that can occur with dataflow vertex.
type VertexChanges<'d when 'd:>IVertexData> = 
    /// New vertex is added.
    | New of MdVertexState<VertexState<'d>>
    /// Vertex is removed.
    | Removed 
    /// Shape of a vertex state probably is changed.
    | ShapeChanged of old:MdVertexState<VertexState<'d>> * current:MdVertexState<VertexState<'d>> * isConnectionChanged:bool 
    /// Some items of a vertex state are changed.
    | Modified of indices:Set<VertexIndex> * old:MdVertexState<VertexState<'d>> * current:MdVertexState<VertexState<'d>> * isConnectionChanged:bool 
    
type Changes<'v,'d when 'v : comparison and 'd:>IVertexData> = Map<'v, VertexChanges<'d>>

type StateUpdate<'v,'d when 'v:comparison and 'v:>IVertex and 'd:>IVertexData> = 
    { State : State<'v,'d>
      Changes : Changes<'v,'d> }

type VertexItem<'v when 'v:comparison and 'v:>IVertex> = 'v * VertexIndex

module StateOperations =
    open Angara.Data

    val add : StateUpdate<'v,'d> -> 'v -> MdMap<int,VertexState<'d>> -> StateUpdate<'v,'d>
    val update : StateUpdate<'v,'d> -> VertexItem<'v> -> VertexState<'d> -> StateUpdate<'v,'d>
    val replace : StateUpdate<'v,'d> -> 'v -> MdMap<int,VertexState<'d>> -> StateUpdate<'v,'d>

