module Angara.StateMachine

open Angara.Graph
open Angara.State
open Angara.Option

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

type Message<'v,'d when 'v:comparison and 'v:>IVertex and 'd:>IVertexData> =
    | Alter of 
        disconnect:  (('v * InputRef) * ('v * OutputRef) option) list *
        remove:      ('v * RemoveStrategy) list *
        merge:       (DataFlowGraph<'v> * DataFlowState<'v,VertexState<'d>>) *
        connect:     AlterConnection<'v> list *
        reply:       ReplyChannel<unit>
    | Start     of 'v * Option<VertexIndex * Option<TimeIndex>> * ReplyChannel<bool>
    | Stop      of 'v
    | Succeeded of 'v * VertexIndex * final: bool * 'd * startTime: TimeIndex // if not "final" the next iterations are coming
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
    /// Returns new state and changes reflecting new vertex.
    let add (state:DataFlowState<_,_>, changes:Changes<_,_>) v vs = state.Add(v, vs), changes.Add(v, VertexChanges.New vs)
    /// Returns new state and changes reflecting the removed vertex.
    let remove (state:DataFlowState<_,_>, changes:Changes<_,_>) v = state.Remove(v), changes.Add(v, VertexChanges.Removed)
    /// Returns new state and changes reflecting that shape of the vertex state probably is changed.
    let replace (state:DataFlowState<_,_>, changes:Changes<_,_>) v vs = 
        let c = match changes.TryFind v with
                | Some(New _) -> New(vs)
                | Some(ShapeChanged(oldvs,_,connChanged)) -> ShapeChanged(oldvs,vs,connChanged)
                | Some(Modified(_,oldvs,_,connChanged)) -> ShapeChanged(oldvs,vs,connChanged)
                | Some(Removed) -> failwith "Removed vertex cannot be modified"
                | None -> ShapeChanged(state.[v],vs,false)
        state.Add(v,vs), changes.Add(v,c)
    /// Returns new state and changes reflecting new status of a vertex for the given index.
    let update (state:DataFlowState<_,_>, changes:Changes<_,_>) v index vis = 
        let vs = state.[v] |> MdMap.add index vis
        let c = match changes.TryFind(v) with
                | Some(New _) -> New(vs)
                | Some(ShapeChanged(oldvs,_,connChanged)) -> ShapeChanged(oldvs,vs,connChanged)
                | Some(Modified(indices,oldvs,_,connChanged)) -> Modified(indices |> Set.add index, oldvs, vs, connChanged)
                | Some(Removed) -> failwith "Removed vertex cannot be modified"
                | None -> Modified(Set.empty |> Set.add index, state.[v], vs, false)
        state.Add(v,vs), changes.Add(v,c)
    /// Returns new state and changes reflecting that the vertex has new input edges.
    let internal vertexInputChanged v (state:DataFlowState<_,_>, changes:Changes<_,_>) = 
        let c = match changes.TryFind(v) with
                | Some(New vs) -> New(vs)
                | Some(ShapeChanged(oldvs,vs,_)) -> ShapeChanged(oldvs,vs,true)
                | Some(Modified(ind,oldvs,vs,_)) -> Modified(ind,oldvs,vs,true)
                | Some(Removed) -> failwith "Removed vertex cannot be modified"
                | None -> Modified(Set.empty,state.[v],state.[v],true)
        state, changes.Add(v,c)

module internal HasStatus = 
    let upToDate = function Complete _ | CompleteStarted _ | CompleteStartRequested _ -> true | _ -> false
    let complete = function Complete _ -> true | _ -> false
    let incompleteReason reason = function Incomplete r when r = reason -> true | _ -> false

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
        Changes.update (state,changes) v index { vis with Status = VertexStatus.Incomplete reason }

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
    let state, changes = Changes.update (state,changes) v index { vis with Status = VertexStatus.CanStart time }
    let state, changes = downstreamVertices (v,index) (graph,state) |> Seq.fold(fun (s,c) vi ->  downstreamToIncomplete vi (graph,s,c) OutdatedInputs) (state,changes)
    state, changes

let internal is1dArray (t:System.Type) = t.IsArray && t.GetArrayRank() = 1

let internal allInputsAssigned (graph:DataFlowGraph<'v>) (v:'v) : bool =
    let n = v.Inputs.Length
    let inedges = graph.Structure.InEdges v |> Seq.toList
    let allAssigned = seq{ 0 .. (n-1) } |> Seq.forall(fun inRef -> (is1dArray v.Inputs.[inRef]) || inedges |> List.exists(fun e -> e.InputRef = inRef))
    allAssigned

type internal ArtefactStatus<'v> = Transient of 'v * VertexIndex | Uptodate | Outdated | Missing

/// Gets either uptodate, outdated or transient status for an artefact at given index.
/// If there is not slice with given index (e.g. if method has unassigned input port), 
/// returns 'outdated'.
let internal getArtefactStatus(v:'v, index:VertexIndex, outRef:OutputRef) (state: DataFlowState<'v,VertexState<'d>>) : ArtefactStatus<'v> =
    match (v,index) |> tryGetStateItem state with
    | Some vis -> 
        match HasStatus.upToDate vis.Status, vis.Data with
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
        match HasStatus.upToDate vis.Status, vis.Data with
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
            Changes.update (state,changes) v index vis
        else
            let vis = { vis with Status = CompleteStartRequested(k) }
            let state,changes = Changes.update (state,changes) v index vis
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

let internal outdatedItems n item = Seq.init n id |> Seq.fold(fun ws j -> ws |> MdMap.add [j] (item j)) MdMap.empty

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
                | Some shape -> outdatedItems shape (fun _ -> VertexState.Outdated)
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
    Changes.replace (state,changes) v vs


let normalize<'v,'d when 'v:comparison and 'v:>IVertex and 'd:>IVertexData> (graph:DataFlowGraph<'v>, state: DataFlowState<'v,VertexState<'d>>) : DataFlowState<'v,VertexState<'d>> * Changes<'v,'d> =
    let dag = graph.Structure
    let state, changes = // adding missing vertex states 
        dag.TopoFold(fun (state:DataFlowState<'v,VertexState<'d>>,changes) v _ -> 
            match state.TryFind v with
            | Some _ -> state,changes
            | None -> 
                let state,changes = Changes.add (state,changes) v (MdMap.scalar VertexState.Unassigned)
                buildVertexState (graph, state, changes) v
            ) (state, noChanges)
        
    // We should start methods, if possible
    let state,changes = 
        dag.Vertices |> Seq.map(fun v -> 
            state.[v] |> MdMap.toSeq |> Seq.filter (fun (index,vis) -> 
                match vis.Status with
                | VertexStatus.Incomplete _ -> true
                | VertexStatus.Complete _ -> false
                | x -> failwith (sprintf "Vertex %O.[%A] has status %O which is not allowed in an initial state" v index x))
            |>Seq.map(fun q -> (v,q))) |> Seq.concat
        |> Seq.fold (fun (state, changes) (v,(i,_)) -> downstreamToCanStartOrIncomplete 0u (v,i) (graph, state, changes)) (state, changes)
    state, changes

////////////////////////////////////////////////////////////////////////////
// 
// Transition
//
////////////////////////////////////////////////////////////////////////////

type internal RemoteItemMergeResult =  StartItem | WaitItem
type internal ArrayIndex = int


/// Decreases length of the vector index to satisfy edge rank.
let internal reduceToEdgeIndex (edge:Edge<_>) (i:VertexIndex) : VertexIndex = i |> List.ofMaxLength (edgeRank edge)

/// Returns list of indices of the reduced artefect, which is a source vertex of the edge, for the target vertex item.
/// Precondition: edge.Type is "Reduce".
let internal enumerateReducedArtefacts (edge:Edge<_>) (i:VertexIndex) (state: DataFlowState<'v,VertexState<'d>>) : VertexIndex list option =
    match state.TryFind edge.Source with
    | Some svs ->
        let r = i.Length
        let items = svs |> MdMap.startingWith i |> MdMap.toSeq |> Seq.filter(fun (j,_) -> j.Length = r + 1) |> Seq.map(fun (j,_) -> j, List.last j) |> Seq.toList
        let n = items |> Seq.map snd |> Seq.max
        let set = items |> Seq.map snd |> Set.ofSeq
        if Seq.init (n+1) (fun i -> set.Contains(i)) |> Seq.forall (fun t -> t) then Some (items |> Seq.map fst |> Seq.toList)
        else None
    | None -> None

/// Enumerates input items of the given complete vertex item.
/// Returns an array with an element for each input port.
/// Each list element contains a list of:
/// * Input edge which exposes input vertex and output port
/// * Slice index of the input vertex
/// * Optional array index, used if the input edge is "Scatter" and the vertex item depends on an array element.
let internal enumerateInputs (graph:DataFlowGraph<'v>, state: DataFlowState<'v,VertexState<'d>>) (v, i : VertexIndex) : (Edge<_> * VertexIndex * ArrayIndex option) list = 
    graph.Structure.InEdges v
    |> Seq.map (fun edge -> 
        let edgeIndex = i |> reduceToEdgeIndex edge
        match edge.Type with
        | OneToOne _ | Collect _ -> [ edge, edgeIndex, None ]
        | Scatter _ -> 
            let j : ArrayIndex = List.last edgeIndex
            [ edge, edgeIndex |> List.removeLast, Some j ]
        | Reduce _ -> 
            match enumerateReducedArtefacts edge edgeIndex state with
            | Some indices -> indices |> Seq.map(fun ri -> edge, ri, None) |> Seq.toList
            | None -> [])
    |> List.concat

let rec internal mergeRemoteItem (ri: SucceededRemoteVertexItem<'v>, items: SucceededRemoteVertexItem<'v> list) (graph:DataFlowGraph<'v>) (state: DataFlowState<'v,VertexState<'d>>, changes) : RemoteItemMergeResult * DataFlowState<'v,VertexState<'d>> * Changes<'v,'d> = 
    let merge (vis:VertexState<'d>) (state,changes) =
        match ri.Execution with
        | Fetch -> 
            let state,changes = Changes.update (state, changes) ri.Vertex ri.Slice { vis with Data = None; Status = CompleteStarted (None, ri.CompletionTime) }      
            WaitItem, state, changes
        | Compute ->
            let tryInput (res,state,changes) (e:Edge<_>,slice,ai:ArrayIndex option) =
                let q,state,changes = 
                    match items |> List.tryFind(fun u -> u.Vertex = e.Source && u.Slice = slice) with
                    | Some u -> mergeRemoteItem (u, items) graph (state,changes)
                    | None -> 
                        match tryGetStateItem state (e.Source,slice) with
                        | Some vis when HasStatus.complete vis.Status -> StartItem, state, changes
                        | _ -> WaitItem, state, changes
                let q' = match res,q with WaitItem,_ | _,WaitItem -> WaitItem | _ -> StartItem
                q', state, changes

            let res,state,changes = enumerateInputs(graph, state) (ri.Vertex, ri.Slice) |> Seq.fold tryInput (StartItem, state, changes)
            let status = match res with StartItem -> CompleteStarted (None, ri.CompletionTime) | WaitItem -> CompleteStartRequested (None)
            let state,changes = Changes.update (state, changes) ri.Vertex ri.Slice { vis with Data = None; Status = status }      
            WaitItem,state,changes

    let mergeMissing v i vs = 
        let vis = VertexState.Outdated 
        let state = state |> Map.add v (vs |> MdMap.add i vis)
        merge vis (state,changes)

    match state.TryFind ri.Vertex with
    | Some vs ->
        match vs |> MdMap.tryFind ri.Slice with 
        | Some vis when HasStatus.complete vis.Status -> StartItem, state, changes
        | Some vis -> merge vis (state,changes)
        | None -> mergeMissing ri.Vertex ri.Slice vs
    | None -> mergeMissing ri.Vertex ri.Slice MdMap.Empty


/// When 'v' succeeds and thus we have its output artefacts, this method updates downstream vertices so their state dimensionality corresponds to the given output.
let internal downstreamShapeFor (outEdges: Edge<_> seq) (v, index:VertexIndex, outShape: int list) (graph:DataFlowGraph<'v>) (state: DataFlowState<'v,VertexState<'d>>, changes) = 
    let dag = graph.Structure
    let w = outEdges |> Seq.choose(fun e -> match e.Type with Scatter _ -> Some e.Target | _ -> None) 
    let r = vertexRank v dag
    let scope = Graph.toSeqSubgraph (fun u _ _ -> (vertexRank u dag) <= r) dag w |>
                Graph.topoSort dag
    scope |> Seq.fold(fun (state,changes) w ->
        match w |> allInputsAssigned graph with
        | false -> // some inputs are unassigned
            match (w,[]) |> tryGetStateItem state with
            | Some(wis) when wis.Status |> HasStatus.incompleteReason IncompleteReason.UnassignedInputs -> (state,changes)
            | Some(wis) -> Changes.replace (state,changes) w (MdMap.scalar { wis with Status = Incomplete(UnassignedInputs) })
            | None -> Changes.replace (state,changes) w (MdMap.scalar { VertexState.Unassigned with Status = Incomplete(UnassignedInputs) })
        | true -> // all inputs are assigned
            let getDimLength u (edgeType:ConnectionType) (outRef: OutputRef) : int option =
                let urank = vertexRank u dag
                if urank < r then None // doesn't depend on the target dimension
                else if urank = r then
                    match edgeType with
                    | Scatter _ -> 
                        opt {
                            let! us = state |> Map.tryFind u
                            let! uis = us |> MdMap.tryFind index
                            let! data = uis.Data
                            return! data.TryGetShape outRef
                        }
                    | _ -> None // doesn't depend on the target dimension
                else // urank > r; u is vectorized for the dimension
                    match state.[u] |> MdMap.tryGet index with
                    | Some selection when not selection.IsScalar -> Some(selection |> MdMap.toShallowSeq |> Seq.length)
                    | _ -> None
                
            let ws = state.[w]
            match ws |> MdMap.tryGet index with
                | Some _ ->
                    let dimLen = 
                        w 
                        |> dag.InEdges 
                        |> Seq.map (fun (inedge:Edge<_>) -> getDimLength inedge.Source inedge.Type inedge.OutputRef) 
                        |> Seq.fold(fun (dimLen:(int*int) option) (inDimLen:int option) ->
                            match dimLen, inDimLen with
                            | Some(minDim,maxDim), Some(inDimLen) -> Some(min minDim inDimLen, max maxDim inDimLen)
                            | Some(dimLen), None -> Some(dimLen)
                            | None, Some(inDimLen) -> Some(inDimLen, inDimLen)
                            | None, None -> None) None
                    let wis = match dimLen with
                                | Some (minDim,maxDim) -> outdatedItems maxDim (fun i -> if i < minDim then VertexState.Outdated else VertexState.Unassigned)
                                | None -> failwith "Unexpected case: the vertex should be downstream of the succeeded vector vertex"
                    Changes.replace (state,changes) w (ws |> MdMap.set index wis)
                | None -> (state,changes) // doesn't depend on the target dimension
        ) (state,changes)


/// When 'v' succeeds and thus we have its output artefacts, this method updates downstream vertices so their state dimensionality corresponds to the given output.
let internal downstreamShape (v, index:VertexIndex, outShape: int list) (graph:DataFlowGraph<'v>) (state: DataFlowState<'v,VertexState<'d>>, changes) = 
    downstreamShapeFor (v |> graph.Structure.OutEdges) (v,index,outShape) graph (state,changes)

/// Rebuilds shapes of vsrc and its downstream when vsrc is invalidated as a whole and becomes incomplete
/// (connect/disconnect).
let internal updateStatuses (vsrc) (graph:DataFlowGraph<'v>, state: DataFlowState<'v,VertexState<'d>>,changes:Changes<'v,'d>) =
    let dag = graph.Structure
    let deps = Graph.toSeqSubgraph (fun v (inedges:Lazy<Edge<_> seq>) _ -> false) dag (seq{ yield vsrc }) |> topoSort dag
    deps |> Seq.fold(fun (s,c) v -> buildVertexState (graph,s,c) v) (state,changes)

type DeferredResponse = unit->unit
let internal noResponse() = ()
let internal response<'a> (r:ReplyChannel<'a>) (v:Response<'a>) : DeferredResponse = fun () -> r(v)

/// Returns true, if the state contains an artefact for the given outRef.
let internal hasOutput outRef (vs:VertexState<'d>) =
    match vs.Data with
    | Some d -> d.Contains outRef
    | None -> false

/// Returns true, for the given vertex state there is a missing output artefact.
let internal isOutputPartial<'v, 'd when 'v : comparison and 'v :> IVertex and 'd :> IVertexData> (v:'v) (vs:VertexState<'d>) =
    v.Outputs |> Seq.mapi (fun i _ -> hasOutput i vs) |> Seq.contains false

/// Returns true, if the state contains data required for a single input of a vertex `v` at `index` having given `input` as
/// input edges of a single method input. 
/// Returns false, if there are no input edges or some of them has missing artefact in the state.
let internal isInputAvailable (graph:DataFlowGraph<'v>) (state: DataFlowState<'v,VertexState<'d>>) (v: 'v) index (input: Edge<'v> seq) =
    let inputHasOutput (edge:Edge<'v>) i =
        match state |> Map.tryFind edge.Source with
        | None -> false
        | Some ws ->
            match ws |> MdMap.tryFind i with
            | None -> false
            | Some wis -> wis |> hasOutput edge.OutputRef 
    // Sames as inputHasOutput, but "i" has rank one less than rank of the source vertex,
    // therefore result is an array of artefacts for all available indices complementing "i".
    let reducedInputHasOutputs (edge:Edge<'v>) (i:VertexIndex) =
       match state |> Map.tryFind edge.Source with
        | None -> false
        | Some ws ->
            let r = i.Length
            let items = 
                ws 
                |> MdMap.startingWith i 
                |> MdMap.toSeq
                |> Seq.filter(fun (i,_) -> i.Length = r + 1) 
                |> Seq.map(fun (j,vis) -> List.last j, vis |> hasOutput edge.OutputRef)
                |> Seq.toList
            if items |> List.forall (fun (_,hasOutput) -> hasOutput) then
                let map = items |> Map.ofList
                let n = items |> Seq.map fst |> Seq.max
                Seq.init (n+1) (fun i -> Map.containsKey i map) |> Seq.contains false |> not
            else false

    match List.ofSeq input with
    | [] -> false

    | edge :: [] -> // single input edge
        let edgeIndex = index |> reduceToEdgeIndex edge
        match edge.Type with
        | OneToOne _ | Collect _ -> inputHasOutput edge edgeIndex
        | Scatter _ -> inputHasOutput edge (edgeIndex |> List.removeLast)
        | Reduce _ -> reducedInputHasOutputs edge edgeIndex

    | edges -> // All multiple input edges have type "Collect" due to type check on connection
        edges
        |> Seq.map (fun e -> inputHasOutput e (index |> reduceToEdgeIndex e))
        |> Seq.contains false
        |> not

/// Returns true, if the state contains artefacts for all of the vertex `v` inputs at the given slice `index`.
let internal areInputsAvailable (graph:DataFlowGraph<'v>) (state: DataFlowState<'v,VertexState<'d>>) (v: 'v) index =
    graph.Structure.InEdges v
    |> Seq.groupBy (fun e -> e.InputRef)
    |> Seq.map(fun (_, inEdges) -> isInputAvailable graph state v index inEdges)
    |> Seq.contains false
    |> not

/// Makes a single execution step by evolving the given state in response to a message.
let internal transition<'v, 'd when 'v : comparison and 'v :> IVertex and 'd :> IVertexData> time (graph:DataFlowGraph<'v>, state: DataFlowState<'v,VertexState<'d>>) (msg:Message<'v,'d>) = // : (State*Changes*DeferredResponse)
    Trace.StateMachine.TraceEvent(System.Diagnostics.TraceEventType.Verbose, 0, "Message received: {0}", msg)
    match msg with
    | RemoteSucceeded (tv, remotes) ->
        let tr = remotes |> List.find(fun rvi -> rvi.Vertex = tv) 
        let res,state,changes = mergeRemoteItem (tr, remotes) graph (state,noChanges)
        let state,changes = downstreamShape (tv, tr.Slice, tr.OutputShape) graph (state, changes)
        let state,changes = downstreamVertices (tv, tr.Slice) (graph,state) |> Seq.fold(fun (s,c) vi ->  downstreamToCanStartOrIncomplete time vi (graph,s,c)) (state,changes)
        
        Trace.StateMachine.TraceEvent(System.Diagnostics.TraceEventType.Verbose, 0, sprintf "RemoteSucceeded: %O with state %O" changes state)
        (graph,state),changes,noResponse
        
    | Alter(toDisconnect,toRemove,toMerge,toConnect,replyChannel) ->
        try
            // Disconnecting existing vertices
            let doDisconnect (graph:DataFlowGraph<'v>, state: DataFlowState<'v,VertexState<'d>>,changes:Changes<'v,'d>) ((v,inpRef):InputPort<'v>,source:OutputPort<'v> option) =                
                let graph = 
                    match source with
                    | None -> 
                        Trace.StateMachine.TraceInformation("Connection to {0}.[{1}] is removed", v.ToString(), inpRef)
                        graph.Disconnect (v,inpRef)                                                                                      
                    | Some(sv, outref) -> 
                        Trace.StateMachine.TraceInformation("Connection to {0}.[{1}] from {2}.[{3}] is removed", v.ToString(), inpRef, sv.ToString(), outref) 
                        graph.Disconnect (v,inpRef, sv,outref) 
                
                let state,changes = updateStatuses v (graph,state,changes)
                let state,changes = state.[v] |> MdMap.toSeq |> Seq.fold (fun (s,c) (i, _) -> downstreamToCanStartOrIncomplete time (v,i) (graph,s,c) |> Changes.vertexInputChanged v) (state,changes)
                graph,state,changes

            let graph,state,changes = toDisconnect |> List.fold doDisconnect (graph,state,noChanges)
            
            // Remove existing vertices
            let doRemove (graph:DataFlowGraph<'v>, state: DataFlowState<'v,VertexState<'d>>,changes:Changes<'v,'d>) (v:'v, strategy: RemoveStrategy) =
                match graph.TryRemove(v) with // None, if v has downstream vertices
                    | Some graph ->
                        Trace.StateMachine.TraceInformation("Vertex {0} removed", v.ToString())                    
                        let state, changes = Changes.remove (state, changes) v
                        graph,state,changes
                    | None -> 
                        Trace.StateMachine.TraceEvent(Trace.Event.Error, 0, sprintf "Vertex %s cannot be removed because it has dependent vertices" (v.ToString()))
                        match strategy with
                        | FailIfHasDependencies -> failwith (sprintf "Vertex %s cannot be removed because it has dependent vertices" (v.ToString()))
                        | DontRemoveIfHasDependencies -> graph,state,changes

            let graph,state,changes = toRemove |> List.fold doRemove (graph,state,changes)

            // Merge with another work
            let graph = DataFlowGraph(graph.Structure.Combine((fst toMerge).Structure))
            let mergeState, _ = normalize toMerge
            let state,changes = mergeState |> Map.fold(fun (state,changes) v vs -> Changes.add (state,changes) v vs) (state,changes)

            // Connect vertices
            let doConnect (graph:DataFlowGraph<'v>, state: DataFlowState<'v,VertexState<'d>>,changes) (c:AlterConnection<'v>) =
                let v, graph = 
                    match c with 
                    | ItemToItem (t,s) -> fst t, graph.Connect t s
                    | ItemToArray (t,s) -> fst t, graph.ConnectItem t s
                    
                // Possible optimization: in certain cases we now that shape of vertex state doesn't change:
                // e.g. if input rank is 0
                let state,changes = updateStatuses v (graph,state,changes) // rank can be same as before connection, but still length of a dimension can be different
                let state,changes = state.[v] |> MdMap.toSeq |> Seq.fold (fun (s,c) (i, _) -> downstreamToCanStartOrIncomplete time (v,i) (graph,s,c) |> Changes.vertexInputChanged v) (state,changes)
                graph,state,changes

            let graph,state,changes = toConnect |> List.fold doConnect (graph,state,changes) 
            (graph,state),changes, response replyChannel (Success())
        with exn -> 
            Trace.StateMachine.TraceEvent(System.Diagnostics.TraceEventType.Error, 0, "Failed to alter the graph: {0}", exn.ToString())
            (graph,state),noChanges, response replyChannel (Response.Exception(exn))

    | Start(v, itemRef, replyChannel) -> 
        match state.TryFind(v) with 
        | None -> (graph, state), noChanges, response replyChannel (Response.Success(false))
        | Some(vs) -> 
            let canStart (vis : VertexState<'d>) = 
                let checkTime time = 
                    match itemRef with
                    | Some (_,Some(expTime)) -> expTime = time
                    | _ -> true
                match vis.Status with
                | VertexStatus.CanStart time -> checkTime time
                | VertexStatus.Complete k (*| VertexStatus.CompleteStartRequested (iteration=k) *) ->
                    if isOutputPartial v vis then not(k.IsSome) // "Transient iterative methods are not yet supported"
                    else k.IsSome // It is a non-transient completed method, if it is iterative we resume iterations
                | _ -> false


            let startItem (state,changes) (index, vis : VertexState<'d>) = 
                match vis.Status with
                | VertexStatus.CanStart t ->
                    Trace.StateMachine.TraceInformation("Method {0}.[{1}] is started", v, index)
                    Changes.update (state, changes) v index ({ vis with Status = VertexStatus.Started(0, time) })

                | VertexStatus.Complete k (*| VertexStatus.CompleteStartRequested (iteration=k) *)->
                    if isOutputPartial v vis then
                        if k.IsSome then failwith "Transient iterative methods are not yet supported"
                        if areInputsAvailable graph state v index then
                            Trace.StateMachine.TraceInformation("Transient method {0}.[{1}] is started", v, index)
                            Changes.update (state, changes) v index ({ vis with Status = VertexStatus.CompleteStarted(None,time) })
                        // Else some input vertex is also transient
                        else 
                            Trace.StateMachine.TraceInformation("Method {0}.[{1}] will be started when its input transient method succeeds", v, index)
                            startTransient time (v,index) graph (state, changes)
                    else // It is a non-transient completed method
                        // if it is iterative we resume iterations
                        match vis.Status with
                        | VertexStatus.Complete (Some iteration) -> 
                            Trace.StateMachine.TraceInformation("Iterative method {0}.[{1}] is resumed from iteration {2}", v, index, iteration)
                            Changes.update (state, changes) v index ({ vis with Status = VertexStatus.Started (iteration,time)})
                        | _ -> state, changes
                | _ -> failwith "Cannot start a method which is not in state 'CanStart'"

            let items = 
                match itemRef with
                | Some(index, _) -> // start individual method 
                    match vs |> MdMap.tryFind index with
                    | Some(vis) -> [ index, vis ]
                    | None -> List.empty // vertex not found
                | None -> // start all methods of the vertex
                    vs |> MdMap.toSeq |> Seq.toList

            match items |> Seq.map snd |> Seq.forall canStart with
            | true ->
                let state,changes = items |> Seq.fold startItem (state,noChanges)
                (graph,state), changes, response replyChannel (Response.Success(true))
            | false -> 
                (graph,state), noChanges, response replyChannel (Response.Success(false))

    | Stop(v) ->
        match state.TryFind(v) with 
        | None -> (graph, state), noChanges, noResponse
        | Some(vs) -> 
            let state,changes = 
                vs 
                |> MdMap.toSeq
                |> Seq.fold(fun (state,changes) (index, vis) -> 
                    match vis.Status with
                    | VertexStatus.Started (k,_) when k = 0 ->
                        downstreamToIncomplete (v, index) (graph, state, changes) IncompleteReason.Stopped
                    | VertexStatus.Started (k,_) ->
                        Changes.update (state,changes) v index { vis with Status = VertexStatus.Complete (Some k)}
                    | _ ->  
                        Trace.StateMachine.TraceInformation("Vertex {0}.[{1}] is not started and therefore cannot be stopped", v, index)
                        state,changes) (state, noChanges)
            (graph, state), changes, noResponse

    | Failed(v, index, failure, expectedStartTime) ->
        match (v, index) |> tryGetStateItem state with 
        | None -> (graph, state), noChanges, noResponse // message is obsolete
        | Some(vis) ->
            match vis.Status with
            | VertexStatus.Started startTime 
            | VertexStatus.Continues (_,_,startTime) when startTime = expectedStartTime ->                    
                Trace.StateMachine.TraceInformation("Method {0}.[{1}] failed: {2}", v, index, failure)
                let state, changes = downstreamToIncomplete (v,index) (graph,state,noChanges) (ExecutionFailed(failure))
                (graph, state), changes, noResponse
            | VertexStatus.CompleteStarted (k,shape,startTime) when startTime = expectedStartTime ->
                Trace.StateMachine.TraceEvent(Trace.Event.Error, 0, "Transient method {0}.[{1}] failed to re-compute: {2}", v, index, failure)
                let state,changes = modifyAt (state, noChanges) v index { vis with Status = VertexStatus.Complete (k,shape) }
                (graph, state), changes, noResponse
            | _ -> 
                Trace.StateMachine.TraceEvent(Trace.Event.Verbose, 0, sprintf "Message 'failed' is ignored in the vertex status %O" vis.Status)
                (graph, state), noChanges, noResponse
                

    | Succeeded(v, index, final, output, expectedStartTime) -> 
        match (v, index) |> tryGetStateItem state with 
        | None -> (graph, state), noChanges, noResponse // message is obsolete
        | Some(vis) ->
            let trace() = Trace.StateMachine.TraceInformation("Method {0}.[{1}] {2}succeeded", v, index, if final then "" else "iteration ")
            let changes:Map<'v,VertexChanges> = noChanges
            let shape = lazy( buildShape output ) // Each list item contains number of elements in a corresponding output array, or zero, if it is not an array.

            let state, changes = 
                match vis.Status with
                | VertexStatus.Started startTime when startTime = expectedStartTime ->
                    trace()                        
                    let state,changes = match final with
                                        | true  -> modifyAt (state, changes) v index {vis with Output = Full(output); Status = VertexStatus.Complete (None,shape.Value)}
                                        | false -> modifyAt (state, changes) v index {vis with Output = Full(output); Status = VertexStatus.Continues (1,shape.Value, startTime)}
                    let state,changes = downstreamShape (v, index, shape.Value) graph (state, changes)
                    downstreamVertices (v,index) (graph,state) |> Seq.fold(fun (s,c) vi ->  downstreamToCanStartOrIncomplete time vi (graph,s,c)) (state,changes)

                | VertexStatus.Continues (k, shapeLast, startTime) when startTime = expectedStartTime ->
                    trace()                        
                    match final with
                    | true -> 
                        modifyAt (state, changes) v index {vis with Status = VertexStatus.Complete (Some k, shapeLast)}
                    | false -> 
                        Trace.StateMachine.TraceInformation("Method {0}.[{1}] continues with iteration {2}", v, index, k+1)
                        
                        let affectedEdges = getAffectedDependencies (v, index, vis) output graph
                        
                        let state,changes = modifyAt (state,changes) v index {vis with Output = Full(output); Status = VertexStatus.Continues (k+1, shape.Value, startTime)}
                        let state,changes = if shape.Value <> shapeLast then downstreamShape (v, index, shape.Value) graph (state, changes) else state,changes

                        downstreamVerticesFor affectedEdges index (graph,state) |> Seq.fold(fun (s,c) vi ->  downstreamToCanStartOrIncomplete time vi (graph,s,c)) (state,changes)

                | VertexStatus.CompleteStarted (k, shape, startTime) when startTime = expectedStartTime ->
                    trace()                        
                    let state,changes = modifyAt (state, noChanges) v index { vis with Output = Full(output); Status = VertexStatus.Complete (k,shape) }

                    downstreamVertices (v,index) (graph,state) |> Seq.fold(fun (state,changes) (v,i) ->  
                        match state.[v].TryGetItem i with
                        | None -> (state,changes) // inputs unassigned  
                        | Some (vis) ->
                            if not (IsUpToDate vis) then downstreamToCanStartOrIncomplete time (v,i) (graph, state, changes)
                            else match vis.Status with | VertexStatus.CompleteStartRequested (k,shape) -> // transient dependent method waits to start
                                                            Trace.StateMachine.TraceEvent(System.Diagnostics.TraceEventType.Verbose, 1, "Starting transient method {0}@{1}...", v, i)
                                                            modifyAt (state, changes) v i { vis with Status = VertexStatus.CompleteStarted (k, shape, time)}
                                                        | _ -> state,changes) (state,changes)
                | _ -> 
                    Trace.StateMachine.TraceEvent(System.Diagnostics.TraceEventType.Verbose, 0, sprintf "Message 'succeeded' is ignored in the vertex status %O" vis.Status)
                    (state,changes)
            (graph, state), changes, noResponse
