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

type VertexState<'d>  = {
    Status : VertexStatus
    Data : 'd
}

type State<'v,'d when 'v:comparison and 'v:>IVertex> =
    { Graph : DataFlowGraph<'v>
      Status : DataFlowState<'v,VertexState<'d>> 
      TimeIndex : TimeIndex }

type VertexChanges<'d> = 
    | New of MdVertexState<VertexState<'d>>
    | Removed 
    | ShapeChanged of old:MdVertexState<VertexState<'d>> * current:MdVertexState<VertexState<'d>> * isConnectionChanged:bool 
    | Modified of indices:Set<VertexIndex> * old:MdVertexState<VertexState<'d>> * current:MdVertexState<VertexState<'d>> * isConnectionChanged:bool 
    
type Changes<'v,'d when 'v : comparison> = Map<'v, VertexChanges<'d>>
let noChanges<'v,'d when 'v : comparison> = Map.empty<'v, VertexChanges<'d>>

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

type Message<'v,'d,'output when 'v:comparison and 'v:>IVertex> =
    | Alter of 
        disconnect:  (('v * InputRef) * ('v * OutputRef) option) list *
        remove:      ('v * RemoveStrategy) list *
        merge:       (DataFlowGraph<'v> * DataFlowState<'v,VertexState<'d>>) *
        connect:     AlterConnection<'v> list *
        reply:       ReplyChannel<unit>
    | Start     of 'v * Option<VertexIndex * Option<TimeIndex>> * ReplyChannel<bool>
    | Stop      of 'v
    | Succeeded of 'v * VertexIndex * final: bool * 'output * startTime: TimeIndex // if not "final" the next iterations are coming
    | Failed    of 'v * VertexIndex * failure: System.Exception * startTime: TimeIndex
    | RemoteSucceeded of RemoteVertexSucceeded<'v> 

[<Interface>]
type IStateMachine<'v,'d when 'v:comparison and 'v:>IVertex> = 
    inherit System.IDisposable
    abstract Start : unit -> unit
    abstract State : State<'v,'d>
    abstract OnChanged : System.IObservable<State<'v,'d> * Changes<'v,'d>>


//////////////////////////////////////////////////
//
// Implementation
//
//////////////////////////////////////////////////

open Angara.Data

let internal add (state:DataFlowState<_,_>, changes:Changes<_,_>) (v, vs) = state.Add(v, vs), changes.Add(v, VertexChanges.New vs)

let internal modifyAt (state:DataFlowState<_,_>, changes:Changes<_,_>) v index vis = 
    let vs = state.[v] |> MdMap.add index vis
    let c = match changes.TryFind(v) with
            | Some(New _) -> New(vs)
            | Some(ShapeChanged(oldvs,_,connChanged)) -> ShapeChanged(oldvs,vs,connChanged)
            | Some(Modified(indices,oldvs,_,connChanged)) -> Modified(indices |> Set.add index, oldvs, vs, connChanged)
            | Some(Removed) -> failwith "Removed vertex cannot be modified"
            | None -> Modified(Set.empty |> Set.add index, state.[v], vs, false)
    state.Add(v,vs), changes.Add(v,c)

/// Builds tree indices of the immediate dependent vertices.
/// Preconditions: either case:
/// (1) v[index] is CanStart
/// (2) v[index] is Uptodate
/// Both guarantees that all dimensions lengths of the vertex scope except at least of last are known.
let rec downstreamVerticesFor (outEdges: Edge<'v> seq) (index:VertexIndex) (graph:DataFlowGraph<'v>,state:DataFlowState<'v,'d>) =   
    outEdges |> Seq.map(fun e -> 
        let baseIndex = 
            match e.Type with
            | OneToOne _ | Collect (_,_) -> index // actual indices will be equal are greater
            | Scatter _ -> index // actual indices will be greater
            | Reduce _ -> List.removeLast index
        // The function precondition guarantee that target vertex state map has items at baseIndex or deeper, 
        // depending on the extra dimensions in respect to the edge scope
        match state.TryFind e.Target with
        | Some tvs -> tvs |> MdMap.startingWith baseIndex |> MdMap.toSeq |> Seq.map (fun (i,_) -> e.Target, i)
        | None -> Seq.empty
        ) |> Seq.concat

/// Builds tree indices of the immediate dependent vertices.
/// Preconditions: either case:
/// (1) v[index] is CanStart
/// (2) v[index] is Uptodate
/// Both guarantees that all dimensions lengths of the vertex scope except at least of last are known.
let rec downstreamVertices(v, index:VertexIndex) (graph:DataFlowGraph<'v>,state:DataFlowState<'v,'d>) =
    downstreamVerticesFor (v |> graph.Structure.OutEdges) index (graph,state)

/// Move given vertex item to "CanStart", increases its version number
let internal makeCanStart time (v, index:VertexIndex) (graph:DataFlowGraph<'v>, state: DataFlowState<'v,VertexState<'d>>, changes) = 
    let vis = state.[v] |> MdMap.find index
    let state, changes = modifyAt (state,changes) v index { vis with Status = VertexStatus.CanStart time }
    let state, changes = downstreamVertices (v,index) (graph,state) |> Seq.fold(fun (s,c) vi ->  downstreamToIncomplete vi (graph,s,c) OutdatedInputs) (state,changes)
    state, changes

/// Updates status of a single index of the vertex status (i.e. scope remains the same)
let rec internal downstreamToCanStartOrIncomplete<'v when 'v : comparison and 'v :> IVertex> 
    time (v:'v, index:VertexIndex) (graph: DataFlowGraph<_>,state: DataFlowState<_,_>, changes: Changes<_,_>) : DataFlowState<_,_> * Changes<_,_> = 
    let (state,changes) = 
        match v.Inputs.Length with
        | 0 -> makeCanStart time (v,index) (graph,state,changes) // can start as it is now
        | _ as n -> 
            let inedges = graph.Structure.InEdges v |> Seq.toList
            let allAssigned = allInputsAssigned graph v
            if allAssigned then 
                    let statuses = inedges |> Seq.map(fun e -> upstreamArtefactStatus (e,index) graph state) |> Seq.concat
                    let transients, outdated, unassigned = statuses |> Seq.fold(fun (transients,outdated,unassigned) status -> 
                        match status with 
                        | Transient (v,vi) -> (v,vi)::transients, outdated, unassigned
                        | Outdated -> (transients, true, unassigned)
                        | Missing  -> (transients, outdated, true)
                        | Uptodate -> (transients, outdated, unassigned)) (List.empty,false,false)
                
                    match transients, outdated||unassigned with
                        (* has transient input, has outdated input *)
                        | _, true -> // there are outdated input artefacts 
                                downstreamToIncomplete (v,index) (graph,state,changes) (if unassigned then UnassignedInputs else OutdatedInputs)

                        | transients, false when transients.Length > 0 -> // there are transient input artefacts
                                (* starting upstream transient methods *)
                                let transients = transients |> Seq.distinct
                                let state,changes = transients |> Seq.fold(fun (s,c) t -> startTransient time t graph (s,c)) (state,changes)
                                downstreamToIncomplete (v,index) (graph,state,changes) TransientInputs

                        | _ -> makeCanStart time (v,index) (graph,state,changes) // can start as it is now
            else // has unassigned inputs
                downstreamToIncomplete (v,index) (graph,state,changes) UnassignedInputs
    state, changes

let normalize<'v,'d when 'v:comparison and 'v:>IVertex> (state : State<'v,'d>) : State<'v,'d> * Changes<'v,'d> =
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
