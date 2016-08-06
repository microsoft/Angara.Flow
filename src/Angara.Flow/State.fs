namespace Angara.States

open Angara
open Angara.Graph

type TimeIndex = uint64

type IncompleteReason = 
    | UnassignedInputs
    | OutdatedInputs
    | ExecutionFailed   of failure: System.Exception
    | Stopped
    | TransientInputs
    override this.ToString() =
        match this with
        | UnassignedInputs -> "Method has unassigned inputs"
        | OutdatedInputs -> "Method has outdated inputs"
        | ExecutionFailed failure -> "Execution failed: " + (match failure with null -> "<na>" | _ -> failure.ToString())
        | Stopped -> "Stopped"
        | TransientInputs -> "Transient inputs"

type OutputShape = int list

type VertexStatus =
    | Final of OutputShape       
    | Final_MissingInputOnly of OutputShape    
    | Final_MissingOutputOnly of OutputShape
    | Final_MissingInputOutput of OutputShape                         
    | Paused of OutputShape 
    | Paused_MissingOutputOnly of OutputShape
    | Paused_MissingInputOnly of OutputShape
    | Paused_MissingInputOutput of OutputShape
    | CanStart                  of time: TimeIndex 
    | Started                   of startTime: TimeIndex      
    | Continues                 of startTime: TimeIndex * outputShape : OutputShape      
    | Reproduces                of startTime: TimeIndex * outputShape : OutputShape
    | ReproduceRequested of OutputShape   
    | Incomplete                of reason: IncompleteReason
   
    member x.IsUpToDate = match x with Final _ | Reproduces _ | ReproduceRequested _ | Paused _ | Continues _ -> true | _ -> false
    member x.IsIncompleteReason reason = match x with Incomplete r when r = reason -> true | _ -> false
    member x.TryGetShape() = 
        match x with
        | Final s
        | Final_MissingInputOnly s    
        | Final_MissingOutputOnly s
        | Final_MissingInputOutput s
        | Paused s 
        | Paused_MissingOutputOnly s
        | Paused_MissingInputOnly s
        | Paused_MissingInputOutput s
        | Continues (_,s)
        | Reproduces (_,s)
        | ReproduceRequested s -> Some s
        | CanStart _
        | Started _
        | Incomplete _ -> None


[<Interface>]
type IVertexData =
    abstract member Shape : OutputShape

type VertexState = 
    { Status : VertexStatus
      Data : IVertexData option
    } with 
    static member Unassigned : VertexState = 
        { Status = VertexStatus.Incomplete (IncompleteReason.UnassignedInputs); Data = None }
    static member Outdated : VertexState = 
        { Status = VertexStatus.Incomplete (OutdatedInputs); Data = None }
    static member Final (data: IVertexData) =
        { Status = VertexStatus.Final (data.Shape); Data = Some data }

type State<'v when 'v:comparison and 'v:>IVertex> =
    { Graph : DataFlowGraph<'v>
      FlowState : DataFlowState<'v, VertexState> 
      TimeIndex : TimeIndex }

type VertexChanges = 
    | New of MdVertexState<VertexState>
    | Removed 
    | ShapeChanged of old:MdVertexState<VertexState> * current:MdVertexState<VertexState> * isConnectionChanged:bool 
    | Modified of indices:Set<VertexIndex> * old:MdVertexState<VertexState> * current:MdVertexState<VertexState> * isConnectionChanged:bool 
    
type Changes<'v when 'v : comparison> = Map<'v, VertexChanges>

type StateUpdate<'v when 'v:comparison and 'v:>IVertex> = 
    { State : State<'v>
      Changes : Changes<'v> }

type VertexItem<'v when 'v:comparison and 'v:>IVertex> = 'v * VertexIndex

[<AutoOpen>]
module StateOperations =
    let normalize (state : State<'v>) : StateUpdate<'v> = // todo
        { State = state; Changes = Map.empty }