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
type IVertexData =
    abstract member Contains : OutputRef -> bool
    abstract member TryGetShape : OutputRef -> int option

type VertexState<'d when 'd:>IVertexData>  = {
    Status : VertexStatus
    Data : 'd option
} with 
    static member Unassigned : VertexState<'d> = 
        { Status = VertexStatus.Incomplete (IncompleteReason.UnassignedInputs); Data = None }
    static member Outdated : VertexState<'d> = 
        { Status = VertexStatus.Incomplete (OutdatedInputs); Data = None }

type State<'v,'d when 'v:comparison and 'v:>IVertex and 'd:>IVertexData> =
    { Graph : DataFlowGraph<'v>
      Status : DataFlowState<'v,VertexState<'d>> 
      TimeIndex : TimeIndex }

type VertexChanges<'d when 'd:>IVertexData> = 
    | New of MdVertexState<VertexState<'d>>
    | Removed 
    | ShapeChanged of old:MdVertexState<VertexState<'d>> * current:MdVertexState<VertexState<'d>> * isConnectionChanged:bool 
    | Modified of indices:Set<VertexIndex> * old:MdVertexState<VertexState<'d>> * current:MdVertexState<VertexState<'d>> * isConnectionChanged:bool 
    
type Changes<'v,'d when 'v : comparison and 'd:>IVertexData> = Map<'v, VertexChanges<'d>>
let noChanges<'v,'d when 'v : comparison and 'd:>IVertexData> = Map.empty<'v, VertexChanges<'d>>

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

type Message<'v,'d,'output when 'v:comparison and 'v:>IVertex and 'd:>IVertexData> =
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
type IStateMachine<'v,'d when 'v:comparison and 'v:>IVertex and 'd:>IVertexData> = 
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

[<RequireQualifiedAccessAttribute>]
module internal Changes =
    let vertexAdded (state:DataFlowState<_,_>, changes:Changes<_,_>) v vs = state.Add(v, vs), changes.Add(v, VertexChanges.New vs)
    let vertexShapeChanged (state:DataFlowState<_,_>, changes:Changes<_,_>) v vs = 
        let c = match changes.TryFind v with
                | Some(New _) -> New(vs)
                | Some(ShapeChanged(oldvs,_,connChanged)) -> ShapeChanged(oldvs,vs,connChanged)
                | Some(Modified(_,oldvs,_,connChanged)) -> ShapeChanged(oldvs,vs,connChanged)
                | Some(Removed) -> failwith "Removed vertex cannot be modified"
                | None -> ShapeChanged(state.[v],vs,false)
        state.Add(v,vs), changes.Add(v,c)
    let vertexStateChanged (state:DataFlowState<_,_>, changes:Changes<_,_>) v index vis = 
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
let rec internal downstreamVerticesFor (outEdges: Edge<'v> seq) (index:VertexIndex) (graph:DataFlowGraph<'v>,state:DataFlowState<'v,'d>) =   
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
let rec internal downstreamVertices(v, index:VertexIndex) (graph:DataFlowGraph<'v>,state:DataFlowState<'v,VertexState<'d>>) =
    downstreamVerticesFor (v |> graph.Structure.OutEdges) index (graph,state)

let internal tryGetStateItem (state:DataFlowState<'v,VertexState<'d>>) (v, i:VertexIndex) : VertexState<'d> option = 
    match state.TryFind v with
    | Some vs -> vs |> MdMap.tryFind i
    | None -> None

/// Moves vertex items those are downstream for the given vertex item, to an incomplete state.
let rec internal downstreamToIncomplete (v, index:VertexIndex) (graph:DataFlowGraph<'v>,state:DataFlowState<'v,VertexState<'d>>,changes) (reason: IncompleteReason) : DataFlowState<'v,VertexState<'d>> * Changes<'v,'d> = 
    let goFurther reason (state, changes) =
        let downstreamOutputEdge (outedge:Edge<_>) index (state, changes) =        
            let index = 
                match outedge.Type with
                | OneToOne(_) | Scatter(_) | Collect(_,_) -> index
                | Reduce(_) -> List.removeLast index
            downstreamToIncomplete (outedge.Target, index) (graph,state,changes) reason
        v |> graph.Structure.OutEdges |> Seq.fold (fun sc e -> downstreamOutputEdge e index sc) (state,changes)        

    let update v (vis:VertexState<'d>) index = 
        Changes.vertexStateChanged (state,changes) v index { vis with Status = VertexStatus.Incomplete reason }

    match (v,index) |> tryGetStateItem state with
    | Some vis ->         
        match vis.Status with
        | VertexStatus.Incomplete r when r = reason -> 
            state, changes // nothing to do
        | VertexStatus.Incomplete _ 
        | VertexStatus.CanStart _
        | VertexStatus.Started _ ->
            update v vis index // downstream is already incomplete
        | VertexStatus.Complete _
        | VertexStatus.CompleteStarted (_, _)
        | VertexStatus.CompleteStartRequested _ ->
            update v vis index |> goFurther IncompleteReason.OutdatedInputs
    | None -> // missing, i.e. already incomplete state,changes
        state,changes

/// Move given vertex item to "CanStart", increases its version number
let internal makeCanStart time (v, index:VertexIndex) (graph:DataFlowGraph<'v>, state: DataFlowState<'v,VertexState<'d>>, changes) = 
    let vis = state.[v] |> MdMap.find index
    let state, changes = Changes.vertexStateChanged (state,changes) v index { vis with Status = VertexStatus.CanStart time }
    let state, changes = downstreamVertices (v,index) (graph,state) |> Seq.fold(fun (s,c) vi ->  downstreamToIncomplete vi (graph,s,c) OutdatedInputs) (state,changes)
    state, changes

let internal is1dArray (t:System.Type) = t.IsArray && t.GetArrayRank() = 1

let internal allInputsAssigned (graph:DataFlowGraph<'v>) (v:'v) : bool =
    let n = v.Inputs.Length
    let inedges = graph.Structure.InEdges v |> Seq.toList
    let allAssigned = seq{ 0 .. (n-1) } |> Seq.forall(fun inRef -> (is1dArray v.Inputs.[inRef]) || inedges |> List.exists(fun e -> e.InputRef = inRef))
    allAssigned

type internal ArtefactStatus<'v> = Transient of 'v * VertexIndex | Uptodate | Outdated | Missing

let internal statusUpToDate (vs:VertexState<_>) = 
    match vs.Status with Complete _ | CompleteStarted _ | CompleteStartRequested _ -> true | _ -> false

/// Gets either uptodate, outdated or transient status for an artefact at given index.
/// If there is not slice with given index (e.g. if method has unassigned input port), 
/// returns 'outdated'.
let internal getArtefactStatus(v:'v, index:VertexIndex, outRef:OutputRef) (state: DataFlowState<'v,VertexState<'d>>) : ArtefactStatus<'v> =
    match (v,index) |> tryGetStateItem state with
    | Some vis -> 
        match statusUpToDate vis, vis.Data with
        | false, _ -> ArtefactStatus.Outdated
        | true, Some data -> 
            match data.Contains outRef with 
            | true -> ArtefactStatus.Uptodate 
            | false -> ArtefactStatus.Transient (v,index)
        | true, None -> ArtefactStatus.Transient (v,index)
    | None -> ArtefactStatus.Missing

/// Gets either uptodate, outdated or transient status for an item (arrayIndex) of an array-artefact at given index.
/// If there is not slice with given index (e.g. if method has unassigned input port), or shape of output array is less than arrayIndex,
/// returns 'missing'.
let internal getArrayItemArtefactStatus (v, index:VertexIndex, arrayIndex:int, outRef:OutputRef) (state: DataFlowState<'v,VertexState<'d>>) : ArtefactStatus<'v> =
    match (v,index) |> tryGetStateItem state with
    | Some vis -> 
        match statusUpToDate vis, vis.Data with
        | false, _ -> ArtefactStatus.Outdated
        | true, Some data -> 
            match data.TryGetShape outRef with
            | Some shape when arrayIndex < shape -> ArtefactStatus.Uptodate
            | Some _ -> ArtefactStatus.Missing // the array has not enough of elements
            | None -> // due to the contract, the vertex output must be 1d-array, so `None` indicates only that vis.Data doesn't contain the output
                ArtefactStatus.Transient (v,index) 
        | true, None -> ArtefactStatus.Transient (v,index)
    | None -> ArtefactStatus.Missing

/// Gets either uptodate, outdated or transient status for an upstream artefact at given slice.
let internal upstreamArtefactStatus (edge: Edge<'v>, index:VertexIndex) (graph: DataFlowGraph<'v>) (state: DataFlowState<'v,VertexState<'d>>) : ArtefactStatus<'v> seq =
    match edge.Type with
    | OneToOne rank -> seq { yield getArtefactStatus (edge.Source, index |> List.ofMaxLength rank, edge.OutputRef) state }
    | Scatter rank when index.Length >= rank -> seq { yield getArrayItemArtefactStatus (edge.Source, index |> List.ofMaxLength (rank-1), index |> List.item (rank-1), edge.OutputRef) state }
    | Scatter _ -> seq { yield ArtefactStatus.Outdated }
    | Reduce rank -> 
        let rs = edge.Source
        let rs_ind = state.[rs] |> MdMap.startingWith (index |> List.ofMaxLength rank) |> MdMap.toSeq |> Seq.map fst |> Seq.toList
        match rs_ind with
        | []  -> seq { yield ArtefactStatus.Outdated }
        | _ -> rs_ind |> Seq.map(fun idx -> getArtefactStatus (edge.Source, idx, edge.OutputRef) state)
    | Collect (_,rank) -> seq { yield getArtefactStatus (edge.Source, index |> List.ofMaxLength rank, edge.OutputRef) state }

/// "v[index]" is a transient vertex in state Complete(k), CompleteStarted(k) or CompleteStartRequested(k)
/// Results: "v[index]" is either in CompleteStarted(k) or CompleteStartRequested(k).
let rec internal startTransient time (v, index:VertexIndex) (graph: DataFlowGraph<'v>) (state: DataFlowState<'v,VertexState<'d>>, changes: Changes<'v,'d>) =
    Trace.StateMachine.TraceEvent(System.Diagnostics.TraceEventType.Verbose, 1, "Transient {0}.[{1}] is queued to start", v, index)
    let vis = state.[v] |> MdMap.find index
    match vis.Status with
    | CompleteStarted (k,_) | CompleteStartRequested k -> state,changes // already started
    | Complete k -> // should be started
        let artefacts = graph.Structure.InEdges v |> Seq.map(fun e -> upstreamArtefactStatus (e,index) graph state) |> Seq.concat
        let transients = artefacts |> Seq.fold(fun t s -> match s with Transient (v,vi) -> (v,vi)::t | _ -> t) List.empty |> Seq.distinct |> Seq.toList
        if transients.Length = 0 then // no transient inputs
            let vis = { vis with Status = CompleteStarted(k,time) }
            Changes.vertexStateChanged (state,changes) v index vis
        else
            let vis = { vis with Status = CompleteStartRequested(k) }
            let state,changes = Changes.vertexStateChanged (state,changes) v index vis
            transients |> Seq.fold(fun (s,c) t -> startTransient time t graph (s,c)) (state,changes)
    | _ -> failwith (sprintf "Unsupported execution status: %s" (vis.Status.ToString()))

/// Updates status of a single index of the vertex status (i.e. scope remains the same)
let rec internal downstreamToCanStartOrIncomplete time (v:'v, index:VertexIndex) (graph:DataFlowGraph<'v>, state: DataFlowState<'v,VertexState<'d>>, changes) = 
    let state,changes =
        match v.Inputs.Length with
        | 0 -> makeCanStart time (v,index) (graph,state,changes) // can start as it is now
        | _ as n -> 
            if allInputsAssigned graph v then 
                let statuses = graph.Structure.InEdges v |> Seq.map(fun e -> upstreamArtefactStatus (e,index) graph state) |> Seq.concat
                let transients, outdated, unassigned = statuses |> Seq.fold(fun (transients,outdated,unassigned) status -> 
                    match status with 
                    | Transient (v,vi) -> (v,vi)::transients, outdated, unassigned
                    | Outdated -> transients, true, unassigned
                    | Missing  -> transients, outdated, true
                    | Uptodate -> transients, outdated, unassigned) (List.empty,false,false)
                
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

/// Builds an incomplete state for the given vertex depending on states of its input vertices.
let internal buildVertexState (graph:DataFlowGraph<'v>, state: DataFlowState<'v,VertexState<'d>>, changes) v =            
    let edgeState (e:Edge<_>) = 
        let vs = state.[e.Source]
        let outdated = MdMap.scalar VertexState.Outdated
        match e.Type with 
        | ConnectionType.OneToOne _ | ConnectionType.Collect _ -> vs
        | ConnectionType.Reduce rank -> vs |> MdMap.trim rank (fun _ _ -> outdated) // e.Source has rank `rank+1`; trimming to `rank`
        | ConnectionType.Scatter rank -> // e.Source is has rank `rank-1`; extending to `rank`
            vs |> MdMap.trim (rank-1) (fun _ m -> // function called for each of vertex state with index of length `rank-1`
                // m is always scalar since e.Source rank is `rank-1`
                match m.AsScalar().Data |> Option.bind(fun data -> data.TryGetShape e.OutputRef) with
                | Some shape -> Seq.init shape id |> Seq.fold(fun ws j -> ws |> MdMap.add [j] VertexState.Outdated) MdMap.empty
                | None -> outdated)
    let vs = 
        match allInputsAssigned graph v with
        | false -> MdMap.scalar VertexState.Unassigned
        | true ->
            let inStates = graph.Structure.InEdges v |> Seq.map(fun e -> edgeState e) |> Seq.toList
            match inStates with
            | [] -> MdMap.scalar VertexState.Outdated
            | [invs] -> invs |> MdMap.map(fun _ -> VertexState.Outdated) 
            | _ -> inStates |> Seq.reduce(MdMap.merge (fun _ -> VertexState.Outdated))
    Changes.vertexShapeChanged (state,changes) v vs


let normalize<'v,'d when 'v:comparison and 'v:>IVertex and 'd:>IVertexData> (smState : State<'v,'d>) : State<'v,'d> * Changes<'v,'d> =
    let dag = smState.Graph.Structure
    let state, changes = // adding missing vertex states 
        dag.TopoFold(fun (state:DataFlowState<'v,VertexState<'d>>,changes) v _ -> 
            match state.TryFind v with
            | Some _ -> state,changes
            | None -> 
                let state,changes = Changes.vertexAdded (state,changes) v (MdMap.scalar VertexState.Unassigned)
                buildVertexState (smState.Graph, state, changes) v
            ) (smState.Status, noChanges)
        
    // We should start methods, if possible
    let state,changes = 
        dag.Vertices |> Seq.map(fun v -> 
            state.[v] |> MdMap.toSeq |> Seq.filter (fun (index,vis) -> 
                match vis.Status with
                | VertexStatus.Incomplete _ -> true
                | VertexStatus.Complete _ -> false
                | x -> failwith (sprintf "Vertex %O.[%A] has status %O which is not allowed in an initial state" v index x))
            |>Seq.map(fun q -> (v,q))) |> Seq.concat
        |> Seq.fold (fun (state, changes) (v,(i,_)) -> downstreamToCanStartOrIncomplete 0u (v,i) (smState.Graph, state, changes)) (state, changes)

    { smState with Status = state; TimeIndex = 0u }, changes
