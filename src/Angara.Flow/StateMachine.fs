module Angara.StateMachine

open Angara.Graph
open Angara.State

type TimeIndex = uint32

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

type VertexStatus =
    | Complete                  of iteration: int option    
    | CompleteStarted           of iteration: int option * startTime: TimeIndex
    | CompleteStartRequested    of iteration: int option
    | Started                   of iteration: int * startTime: TimeIndex
    | Incomplete                of reason: IncompleteReason
    | CanStart                  of time: TimeIndex    
    override this.ToString() =
        match this with
        | Complete(Some it) -> sprintf "Complete %O " it
        | Complete(None) -> "Complete None"
        | CompleteStarted(Some it, time) -> sprintf "CompleteStarted %O at %d" it time
        | CompleteStarted(None, time) -> sprintf "CompleteStarted None at %d" time
        | CompleteStartRequested(Some it) -> sprintf "CompleteStartedRequested %O" it
        | CompleteStartRequested(None) -> "CompleteStartedRequested None"
        | Incomplete r -> "Incomplete " + r.ToString()
        | CanStart time -> sprintf "CanStart since %d" time
        | Started (it,time) -> sprintf "Started at %d, iteration %d" time it

[<Interface>]
type IVertexState =
    abstract member Status : VertexStatus

type State<'v,'vs when 'v:comparison and 'v:>IVertex and 'vs:>IVertexState> =
    { Graph : DataFlowGraph<'v>
      Status : DataFlowState<'v,'vs> 
      TimeIndex : TimeIndex }

type VertexChanges<'vs> = 
    | New of VertexState<'vs>
    | Removed 
    | ShapeChanged of old:VertexState<'vs> * current:VertexState<'vs> * isConnectionChanged:bool 
    | Modified of indices:Set<VertexIndex> * old:VertexState<'vs> * current:VertexState<'vs> * isConnectionChanged:bool 
    
type Changes<'v,'vs when 'v : comparison> = Map<'v, VertexChanges<'vs>>
let noChanges<'v,'vs when 'v : comparison> = Map.empty<'v, VertexChanges<'vs>>

type Response<'a> =
    | Success       of 'a
    | Exception     of System.Exception
type ReplyChannel<'a> = Response<'a> -> unit

type AlterConnection<'v> =
    | ItemToItem    of target: ('v * InputRef) * source: ('v * OutputRef)
    | ItemToArray   of target: ('v * InputRef) * source: ('v * OutputRef)

type RemoveStrategy = FailIfHasDependencies | DontRemoveIfHasDependencies

type RemoteVertexExecution = 
    | Fetch 
    | Compute

type SucceededRemoteVertexItem<'v> = 
    { Vertex: 'v 
      Slice: VertexIndex
      OutputShape: int list 
      CompletionTime: TimeIndex
      Execution: RemoteVertexExecution }

type RemoteVertexSucceeded<'v> = 'v * SucceededRemoteVertexItem<'v> list

type Message<'v,'vs,'output when 'v:comparison and 'v:>IVertex and 'vs:>IVertexState> =
    | Alter of 
        disconnect:  (('v * InputRef) * ('v * OutputRef) option) list *
        remove:      ('v * RemoveStrategy) list *
        merge:       (DataFlowGraph<'v> * DataFlowState<'v,'vs>) *
        connect:     AlterConnection<'v> list *
        reply:       ReplyChannel<unit>
    | Start     of 'v * Option<VertexIndex * Option<TimeIndex>> * ReplyChannel<bool>
    | Stop      of 'v
    | Succeeded of 'v * VertexIndex * final: bool * 'output * startTime: TimeIndex // if not "final" the next iterations are coming
    | Failed    of 'v * VertexIndex * failure: System.Exception * startTime: TimeIndex
    | RemoteSucceeded of RemoteVertexSucceeded<'v> 

[<Interface>]
type IStateMachine<'v,'vs when 'v:comparison and 'v:>IVertex and 'vs:>IVertexState> = 
    inherit System.IDisposable
    abstract Start : unit -> unit
    abstract State: State<'v,'vs>
    abstract OnChanged : System.IObservable<State<'v,'vs> * Changes<'v,'vs>>


//////////////////////////////////////////////////
//
// Implementation
//
//////////////////////////////////////////////////

open Angara.Data

let internal add (state:DataFlowState<_,_>, changes:Changes<_,_>) (v, vs) = state.Add(v,vs), changes.Add(v, VertexChanges.New vs)

let normalize<'v,'vs when 'v:comparison and 'v:>IVertex and 'vs:>IVertexState> (state : State<'v,'vs>) : State<'v,'vs> * Changes<'v,'vs> =
    // Initial state
    let dag = state.Graph.Structure
        
    let status, changes = // adding missing vertex states 
        dag.TopoFold(fun (status:DataFlowState<'v,'vs>,changes) v _ -> 
            match status.TryFind v with
            | Some _ -> status,changes
            | None -> 
                let status,changes = add (status,changes) (v, MdMap.ofItem VertexItemState.Unassigned)
                buildVertexState (graph,state,changes) v
            ) (state.Status, noChanges)
        
    // We should start methods, if possible
    let state,changes = 
        dag.Vertices |> Seq.map(fun v -> 
            state.[v].ToSeq() |> Seq.filter (fun (index,vis) -> 
                match vis.Status with
                | ExecutionStatus.Incomplete _ -> true
                | ExecutionStatus.Complete _ -> false
                | x -> failwith (sprintf "Vertex [%O@%O] has status %O which is not allowed in an initial state" v index x))
            |>Seq.map(fun q -> (v,q))) |> Seq.concat
        |> Seq.fold (fun (state, changes) (v,(i,vis)) -> downstreamToCanStartOrIncomplete 0u (v,i) (graph, state, changes)) (state, Map.empty)

    //let changes = state |> Map.fold(fun (ch:Changes) v vs -> match vs.Status with ExecutionStatus.CanStart -> ch.Add(v,VertexChanges.New vs) | _ -> ch) changes
    ((graph,state),changes)
