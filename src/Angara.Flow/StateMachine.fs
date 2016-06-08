namespace Angara

open Angara.Graph
open Angara.Option

module StateMachine =
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

    type VertexStatus =
        | Final                        
        | Paused 
        | CanStart of time: TimeIndex 
        | Started of startTime: TimeIndex  
        | Continues of startTime: TimeIndex  
        | Reproduces of startTime: TimeIndex
        | ReproduceRequested   
        | Incomplete of reason: IncompleteReason
  
        override this.ToString() =
            match this with
            | Final -> "Final"
            | Reproduces time -> sprintf "Reproduces at %d" time
            | ReproduceRequested -> "ReproduceRequested"
            | Incomplete r -> sprintf "Incomplete %O" r
            | CanStart time -> sprintf "CanStart since %d" time
            | Paused -> "Paused"
            | Continues _ -> "Continues"
            | Started time -> sprintf "Started at %d" time
        member x.IsUpToDate = match x with Final | Reproduces _ | ReproduceRequested _ | Paused | Continues _ -> true | _ -> false
        member x.IsIncompleteReason reason = match x with Incomplete r when r = reason -> true | _ -> false

    type VertexState<'d>  = {
        Status : VertexStatus
        Data : 'd option
    } with 
        static member Unassigned : VertexState<'d> = 
            { Status = VertexStatus.Incomplete (IncompleteReason.UnassignedInputs); Data = None }
        static member Outdated : VertexState<'d> = 
            { Status = VertexStatus.Incomplete (OutdatedInputs); Data = None }
        static member Complete (d: 'd) =
            { Status = VertexStatus.Final; Data = Some d }

    type State<'v,'d when 'v:comparison and 'v:>IVertex> =
        { Graph : DataFlowGraph<'v>
          FlowState : DataFlowState<'v,VertexState<'d>> 
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

    type AlterMessage<'v,'d when 'v:comparison and 'v:>IVertex> =
        { Disconnect:  (('v * InputRef) * ('v * OutputRef) option) list 
          Remove:      ('v * RemoveStrategy) list 
          Merge:       DataFlowGraph<'v> * DataFlowState<'v,VertexState<'d>>
          Connect:     AlterConnection<'v> list }

    type StartMessage<'v> =
        { Vertex: 'v
          Index: VertexIndex
          CanStartTime: TimeIndex option }
   
    type SucceededResult<'d> = 
        | IterationResult of result:'d
        | NoMoreIterations

    type SucceededMessage<'v,'d> =
        { Vertex: 'v
          Index: VertexIndex          
          Result: SucceededResult<'d>
          StartTime: TimeIndex }

    type FailedMessage<'v> =
        { Vertex: 'v
          Index: VertexIndex
          Failure: exn
          StartTime: TimeIndex }


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

    type Message<'v,'d when 'v:comparison and 'v:>IVertex> =
        | Alter         of AlterMessage<'v,'d> * reply: ReplyChannel<unit>
        | Start         of StartMessage<'v> * ReplyChannel<bool>
        | Stop          of 'v
        | Succeeded     of SucceededMessage<'v,'d>
        | Failed        of FailedMessage<'v>
        | RemoteSucceeded of RemoteVertexSucceeded<'v> 

    [<Interface>]
    type IVertexData =
        abstract member Contains : OutputRef -> bool
        abstract member TryGetShape : OutputRef -> int option


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

        match state |> DataFlowState.tryGet (v,index) with
        | Some vis ->         
            match vis.Status with
            | VertexStatus.Incomplete r when r = reason -> 
                state, changes // nothing to do
            | VertexStatus.Incomplete _ 
            | VertexStatus.CanStart _
            | VertexStatus.Started _ ->
                update v vis index // downstream is already incomplete
            | VertexStatus.Final
            | VertexStatus.Paused _
            | VertexStatus.Reproduces _
            | VertexStatus.ReproduceRequested
            | VertexStatus.Continues _ ->
                update v vis index |> goFurther IncompleteReason.OutdatedInputs
        | None -> // missing, i.e. already incomplete state,changes
            state,changes

    /// Move given vertex item to "CanStart"
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
    let internal getArtefactStatus<'v,'d when 'v:comparison and 'v:>IVertex and 'd:>IVertexData>(v:'v, index:VertexIndex, outRef:OutputRef) (state: DataFlowState<'v,VertexState<'d>>) : ArtefactStatus<'v> =
        match state |> DataFlowState.tryGet (v,index) with
        | Some vis -> 
            match vis.Status.IsUpToDate, vis.Data with
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
    let internal getArrayItemArtefactStatus<'v,'d when 'v:comparison and 'v:>IVertex and 'd:>IVertexData> (v, index:VertexIndex, arrayIndex:int, outRef:OutputRef) (state: DataFlowState<'v,VertexState<'d>>) : ArtefactStatus<'v> =
        match state |> DataFlowState.tryGet (v,index) with
        | Some vis -> 
            match vis.Status.IsUpToDate, vis.Data with
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

    /// "v[index]" is a transient vertex in state Complete, Reproduces or ReproduceRequested.
    /// Results: "v[index]" is either in Reproduces or ReproduceRequested.
    let rec internal startTransient time (v, index:VertexIndex) (graph: DataFlowGraph<'v>) (state: DataFlowState<'v,VertexState<'d>>, changes: Changes<'v,'d>) =
        Trace.StateMachine.TraceEvent(System.Diagnostics.TraceEventType.Verbose, 1, "Transient {0}.[{1}] is queued to start", v, index)
        let vis = state.[v] |> MdMap.find index
        match vis.Status with
        | Reproduces _ | ReproduceRequested -> state,changes // already started
        | Final _ -> // should be started
            let artefacts = graph.Structure.InEdges v |> Seq.map(fun e -> upstreamArtefactStatus (e,index) graph state) |> Seq.concat
            let transients = artefacts |> Seq.fold(fun t s -> match s with Transient (v,vi) -> (v,vi)::t | _ -> t) List.empty |> Seq.distinct |> Seq.toList
            if transients.Length = 0 then // no transient inputs
                let vis = { vis with Status = Reproduces time }
                Changes.update (state,changes) v index vis
            else
                let vis = { vis with Status = ReproduceRequested }
                let state,changes = Changes.update (state,changes) v index vis
                transients |> Seq.fold(fun (s,c) t -> startTransient time t graph (s,c)) (state,changes)
        | _ -> failwith (sprintf "Unexpected execution status: %s" (vis.Status.ToString()))

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
    let internal buildVertexState<'v,'d when 'v:comparison and 'v:>IVertex and 'd:>IVertexData> (graph:DataFlowGraph<'v>, state: DataFlowState<'v,VertexState<'d>>, changes) v =            
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
                    | VertexStatus.Final _ -> false
                    | x -> failwith (sprintf "Vertex %O.[%A] has status %O which is not allowed in an initial state" v index x))
                |>Seq.map(fun q -> (v,q))) |> Seq.concat
            |> Seq.fold (fun (state, changes) (v,(i,_)) -> downstreamToCanStartOrIncomplete 0UL (v,i) (graph, state, changes)) (state, changes)
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
                let state,changes = Changes.update (state, changes) ri.Vertex ri.Slice { vis with Data = None; Status = Reproduces ri.CompletionTime }      
                WaitItem, state, changes
            | Compute ->
                let tryInput (res,state,changes) (e:Edge<_>,slice,ai:ArrayIndex option) =
                    let q,state,changes = 
                        match items |> List.tryFind(fun u -> u.Vertex = e.Source && u.Slice = slice) with
                        | Some u -> mergeRemoteItem (u, items) graph (state,changes)
                        | None -> 
                            match state |> DataFlowState.tryGet (e.Source,slice) with
                            | Some vis when vis.Status.IsFinal -> StartItem, state, changes
                            | _ -> WaitItem, state, changes
                    let q' = match res,q with WaitItem,_ | _,WaitItem -> WaitItem | _ -> StartItem
                    q', state, changes

                let res,state,changes = enumerateInputs(graph, state) (ri.Vertex, ri.Slice) |> Seq.fold tryInput (StartItem, state, changes)
                let status = match res with StartItem -> Reproduces ri.CompletionTime | WaitItem -> ReproduceRequested 
                let state,changes = Changes.update (state, changes) ri.Vertex ri.Slice { vis with Data = None; Status = status }      
                WaitItem,state,changes

        let mergeMissing v i vs = 
            let vis = VertexState.Outdated 
            let state = state |> Map.add v (vs |> MdMap.add i vis)
            merge vis (state,changes)

        match state.TryFind ri.Vertex with
        | Some vs ->
            match vs |> MdMap.tryFind ri.Slice with 
            | Some vis when vis.Status.IsCompleted -> StartItem, state, changes
            | Some vis -> merge vis (state,changes)
            | None -> mergeMissing ri.Vertex ri.Slice vs
        | None -> mergeMissing ri.Vertex ri.Slice MdMap.Empty


    /// When 'v' succeeds and thus we have its output artefacts, this method updates downstream vertices so their state dimensionality corresponds to the given output.
    let internal downstreamShapeFor<'v,'d when 'v:comparison and 'v:>IVertex and 'd:>IVertexData> (outEdges: Edge<_> seq) (v, index:VertexIndex) (graph:DataFlowGraph<'v>) (state: DataFlowState<'v,VertexState<'d>>, changes) = 
        let dag = graph.Structure
        let w = outEdges |> Seq.choose(fun e -> match e.Type with Scatter _ -> Some e.Target | _ -> None) 
        let r = vertexRank v dag
        let scope = Graph.toSeqSubgraph (fun u _ _ -> (vertexRank u dag) <= r) dag w |>
                    Graph.topoSort dag
        scope |> Seq.fold(fun (state: DataFlowState<'v,VertexState<'d>>,changes) w ->
            match w |> allInputsAssigned graph with
            | false -> // some inputs are unassigned
                match state |> DataFlowState.tryGet (w,[]) with
                | Some(wis) when wis.Status.IsIncompleteReason IncompleteReason.UnassignedInputs -> (state,changes)
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
    let internal downstreamShape<'v,'d when 'v:comparison and 'v:>IVertex and 'd:>IVertexData> (v, index:VertexIndex) (graph:DataFlowGraph<'v>) (state: DataFlowState<'v,VertexState<'d>>, changes) = 
        downstreamShapeFor (v |> graph.Structure.OutEdges) (v,index) graph (state,changes)

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
    let internal hasOutput<'d when 'd:>IVertexData> outRef (vs:VertexState<'d>) =
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


    /// Returns an array of some of the output edges for the given vertex and index, such that these edges
    /// go from the vertex outputs that are changed if compare their current state `vis` and the given `data`.
    let internal getAffectedDependencies<'v, 'd when 'v : comparison and 'v :> IVertex and 'd :> IVertexData>  (v : 'v, index : VertexIndex) (currentData : 'd option) (newData : 'd) (graph : DataFlowGraph<_>) (matchOutput:'d->'d->OutputRef->bool) : Edge<_> array =
        let edges = graph.Structure.OutEdges v |> Seq.toArray
        match currentData with
        | Some output ->
            seq{ 
                for i in 0 .. v.Outputs.Length-1 do
                    if matchOutput output newData i then
                        Trace.StateMachine.TraceEvent(Trace.Event.Verbose, 3, "Method {0}.[{1}], output {2} matches previous output; downstream is unaffected", v, index, i)
                        yield Seq.empty
                    else
                        yield edges |> Seq.filter (fun e -> e.OutputRef = i)
            } |> Seq.concat |> Seq.toArray       
        | None -> edges

    /// Makes a single execution step by evolving the given state in response to a message.
    let internal transition<'v, 'd when 'v : comparison and 'v :> IVertex and 'd :> IVertexData> time (graph:DataFlowGraph<'v>, state: DataFlowState<'v,VertexState<'d>>) (msg:Message<'v,'d>) (matchOutput:'d->'d->OutputRef->bool) = // : (State*Changes*DeferredResponse)
        Trace.StateMachine.TraceEvent(System.Diagnostics.TraceEventType.Verbose, 0, "Message received: {0}", msg)
        match msg with
        | RemoteSucceeded (tv, remotes) ->
            let tr = remotes |> List.find(fun rvi -> rvi.Vertex = tv) 
            let res,state,changes = mergeRemoteItem (tr, remotes) graph (state,noChanges)
            let state,changes = downstreamShape (tv, tr.Slice) graph (state, changes)
            let state,changes = downstreamVertices (tv, tr.Slice) (graph,state) |> Seq.fold(fun (s,c) vi ->  downstreamToCanStartOrIncomplete time vi (graph,s,c)) (state,changes)
        
            Trace.StateMachine.TraceEvent(System.Diagnostics.TraceEventType.Verbose, 0, sprintf "RemoteSucceeded: %O with state %O" changes state)
            (graph,state),changes,noResponse
        
        | Alter(batch,replyChannel) ->
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

                let graph,state,changes = batch.Disconnect |> List.fold doDisconnect (graph,state,noChanges)
            
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

                let graph,state,changes = batch.Remove |> List.fold doRemove (graph,state,changes)

                // Merge with another work
                let graph = DataFlowGraph(graph.Structure.Combine((fst batch.Merge).Structure))
                let mergeState, _ = normalize batch.Merge
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

                let graph,state,changes = batch.Connect |> List.fold doConnect (graph,state,changes) 
                (graph,state),changes, response replyChannel (Success())
            with exn -> 
                Trace.StateMachine.TraceEvent(System.Diagnostics.TraceEventType.Error, 0, "Failed to alter the graph: {0}", exn.ToString())
                (graph,state),noChanges, response replyChannel (Response.Exception(exn))

        | Start(m, replyChannel) -> 
            let v = m.Vertex
            match state.TryFind(v) with 
            | None -> (graph, state), noChanges, response replyChannel (Response.Success(false))
            | Some vs -> 
//                let canStart (vis : VertexState<'d>) = 
//                    let checkTime time = 
//                        match m.CanStartTime with
//                        | Some (expTime) -> expTime = time
//                        | _ -> true
//                    match vis.Status with
//                    | VertexStatus.CanStart time -> checkTime time
//                    | VertexStatus.Final _ -> true
//                    | _ -> false
//
//                let startItem (state,changes) (index, vis : VertexState<'d>) = 
//                    match vis.Status with
//                    | VertexStatus.CanStart t ->
//                        Trace.StateMachine.TraceInformation("Method {0}.[{1}] is started", v, index)
//                        Changes.update (state, changes) v index ({ vis with Status = VertexStatus.Started time })
//
//                    | VertexStatus.Final ->
//                        if isOutputPartial v vis then // then 'Start' reproduces the output artefacts
//                            if areInputsAvailable graph state v index then
//                                Trace.StateMachine.TraceInformation("Transient method {0}.[{1}] is started", v, index)
//                                Changes.update (state, changes) v index ({ vis with Status = VertexStatus.Reproduces time })                            
//                            else // Else some input vertex is also transient, waiting until it is reproduced in turn.
//                                Trace.StateMachine.TraceInformation("Method {0}.[{1}] will be started when its input transient method succeeds", v, index)
//                                startTransient time (v,index) graph (state, changes)
//                        else // It is a non-transient completed method, 'Start' continues the execution.
//                            Trace.StateMachine.TraceInformation("Iterative method {0}.[{1}] is to be continued...", v, index)
//                            Changes.update (state, changes) v index ({ vis with Status = VertexStatus.Continues time})
//
//                    | _ -> failwith "Cannot start a method which has status other than 'CanStart' or 'Complete'"

                let tryStart (state,changes) (index, vis : VertexState<'d>) = 
                    let checkTime time = 
                        match m.CanStartTime with
                        | Some (expTime) -> expTime = time
                        | _ -> true

                    match vis.Status with
                    | VertexStatus.CanStart t when checkTime t ->
                        Trace.StateMachine.TraceInformation("Method {0}.[{1}] is started", v, index)
                        Changes.update (state, changes) v index ({ vis with Status = VertexStatus.Started time }) |> Some

                    | VertexStatus.CanStart _ -> None

                    | VertexStatus.Final ->
                        if isOutputPartial v vis then // then 'Start' reproduces the output artefacts
                            if areInputsAvailable graph state v index then
                                Trace.StateMachine.TraceInformation("Transient method {0}.[{1}] is started", v, index)
                                Changes.update (state, changes) v index ({ vis with Status = VertexStatus.Reproduces time })                            
                            else // Else some input vertex is also transient, waiting until it is reproduced in turn.
                                Trace.StateMachine.TraceInformation("Method {0}.[{1}] will be started when its input transient method succeeds", v, index)
                                startTransient time (v,index) graph (state, changes)
                        else // It is a non-transient completed method, 'Start' continues the execution.
                            Trace.StateMachine.TraceInformation("Iterative method {0}.[{1}] is to be continued...", v, index)
                            Changes.update (state, changes) v index ({ vis with Status = VertexStatus.Continues time})

                    | _ -> failwith "Cannot start a method which has status other than 'CanStart' or 'Complete'"

                match vs |> MdMap.tryFind m.Index with
                | Some vis when canStart vis -> // vertex slice found
                    let state,changes = startItem (state,noChanges) (m.Index, vis)
                    (graph,state), changes, response replyChannel (Response.Success(true))
                | Some _ | None -> 
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
                        | VertexStatus.Started _ ->
                            downstreamToIncomplete (v, index) (graph, state, changes) IncompleteReason.Stopped
                        | VertexStatus.Continues _ ->
                            Changes.update (state,changes) v index { vis with Status = VertexStatus.Complete }
                        | _ ->  
                            Trace.StateMachine.TraceInformation("Vertex {0}.[{1}] is not started and therefore cannot be stopped", v, index)
                            state,changes) (state, noChanges)
                (graph, state), changes, noResponse

        | Failed(m) ->
            let v,index = m.Vertex,m.Index
            match state |> DataFlowState.tryGet (v,index) with 
            | None -> (graph, state), noChanges, noResponse // message is obsolete
            | Some(vis) ->
                match vis.Status with
                | VertexStatus.Started startTime when startTime = m.StartTime ->                    
                    Trace.StateMachine.TraceInformation("Method {0}.[{1}] failed to execute", v, index, m.Failure)
                    let state, changes = downstreamToIncomplete (v,index) (graph,state,noChanges) (ExecutionFailed(m.Failure))
                    (graph, state), changes, noResponse
                | VertexStatus.Continues startTime when startTime = m.StartTime ->
                    Trace.StateMachine.TraceEvent(Trace.Event.Error, 0, "Transient method {0}.[{1}] failed to execute: {2}", v, index, m.Failure)
                    let state,changes = Changes.update (state, noChanges) v index { vis with Status = VertexStatus.Complete }
                    (graph, state), changes, noResponse
                | VertexStatus.Reproduces startTime when startTime = m.StartTime ->
                    Trace.StateMachine.TraceEvent(Trace.Event.Error, 0, "Transient method {0}.[{1}] failed to reproduce: {2}", v, index, m.Failure)
                    let state,changes = Changes.update (state, noChanges) v index { vis with Status = VertexStatus.Complete }
                    (graph, state), changes, noResponse
                | _ -> 
                    Trace.StateMachine.TraceEvent(Trace.Event.Verbose, 0, sprintf "Message 'failed' is ignored in the vertex status %O" vis.Status)
                    (graph, state), noChanges, noResponse

        | Succeeded(m) -> 
            let v,index = m.Vertex,m.Index
            match state |> DataFlowState.tryGet (v,index) with 
            | None -> (graph, state), noChanges, noResponse // message is obsolete
            | Some(vis) ->
                let changes = noChanges
                let state, changes = 
                    match vis.Status with
                    | VertexStatus.Started startTime when startTime = m.StartTime ->                        
                        match m.Result with
                        | NoMoreIterations -> failwith "There must be at least one iteration before the 'no more iteration' message is received"
                        | IterationResult result ->
                            Trace.StateMachine.TraceInformation("Method {0}.[{1}] first iteration succeeded", v, index)
                            let state,changes = 
                                Changes.update (state, changes) v index { vis with Data = Some result; Status = VertexStatus.Started startTime }
                                |> downstreamShape (v, index) graph
                            let affectedEdges = v |> graph.Structure.OutEdges
                            downstreamVerticesFor affectedEdges index (graph,state) |> Seq.fold(fun (s,c) vi -> downstreamToCanStartOrIncomplete time vi (graph,s,c)) (state,changes)
                    
                    | VertexStatus.Continues startTime when startTime = m.StartTime ->                        
                        match m.Result with
                        | NoMoreIterations -> 
                            Trace.StateMachine.TraceInformation("Method {0}.[{1}] succeeded", v, index)
                            Changes.update (state, changes) v index { vis with Status = VertexStatus.Complete }
                        | IterationResult result ->
                            Trace.StateMachine.TraceInformation("Method {0}.[{1}] iteration succeeded", v, index)
                            let state,changes = Changes.update (state, changes) v index { vis with Data = Some result }
                            let affectedEdges =
                                // The previous iteration already moved the dependent vertices to a proper state,
                                // and if some outputs are not changed since that iteration we can leave them as they are.
                                getAffectedDependencies (v, index) vis.Data result graph matchOutput |> Seq.ofArray                        
                            downstreamVerticesFor affectedEdges index (graph,state) |> Seq.fold(fun (s,c) vi -> downstreamToCanStartOrIncomplete time vi (graph,s,c)) (state,changes)
                    
                    | VertexStatus.Reproduces startTime when startTime = m.StartTime ->                        
                        match m.Result with 
                        | IterationResult result ->
                            Trace.StateMachine.TraceInformation("Method {0}.[{1}] has reproduced its outputs", v, index)                
                            let state,changes = Changes.update (state, noChanges) v index { vis with Data = Some result; Status = VertexStatus.Complete }
                            downstreamVertices (v,index) (graph,state) |> Seq.fold(fun (state,changes) (v,i) ->  
                                match state.[v] |> MdMap.tryFind i with
                                | None -> (state,changes) // inputs unassigned
                                | Some vis when vis.Status.IsUpToDate ->
                                    match vis.Status with 
                                    | VertexStatus.ReproduceRequested -> // transient dependent method waits to start
                                        Trace.StateMachine.TraceEvent(System.Diagnostics.TraceEventType.Verbose, 1, "Reproducing the transient method {0}.[{1}]...", v, i)
                                        Changes.update (state, changes) v i { vis with Status = VertexStatus.Reproduces time }
                                    | VertexStatus.ContinueRequested -> // waits to start
                                        Trace.StateMachine.TraceEvent(System.Diagnostics.TraceEventType.Verbose, 1, "Starting the method {0}.[{1}] which waited for its transient input.", v, i)
                                        Changes.update (state, changes) v i { vis with Status = VertexStatus.Continues time }
                                    | _ -> state,changes
                                | Some _ ->
                                    downstreamToCanStartOrIncomplete time (v,i) (graph, state, changes)) (state,changes)
                        | NoMoreIterations -> failwith "Unexpected message received: a method with the status 'Reproduces' doesn't accept message 'Succeeded NoMoreIterations'"
                    | _ -> 
                        Trace.StateMachine.TraceEvent(System.Diagnostics.TraceEventType.Verbose, 0, sprintf "Message 'succeeded' is ignored in the vertex status %A" vis.Status)
                        (state,changes)
                (graph, state), changes, noResponse

open StateMachine

[<Sealed>]
type StateMachine<'v,'d when 'v:comparison and 'v:>IVertex and 'd:>IVertexData> private (source:System.IObservable<Message<'v, 'd>>, initialState:DataFlowGraph<'v> * DataFlowState<'v,VertexState<'d>>, matchOutput:'d->'d->OutputRef->bool) =
    let obs = Angara.Observable.ObservableSource<State<'v,'d>*Changes<'v,'d>>()
    let mutable agent : Angara.MailboxProcessor.ILinearizingAgent<Message<'v,'d>> option = None
    let mutable unsubs : System.IDisposable = null
    let mutable lastState = { Graph = fst initialState; FlowState = snd initialState; TimeIndex = 0UL }
        
    let messageHandler (msg:Message<'v,'d>) (state:State<'v,'d>) = 
        let time = state.TimeIndex + 1UL
        let (graph,status), changes, reply = transition time (state.Graph, state.FlowState) msg matchOutput
        let state = { Graph = graph; FlowState = status; TimeIndex = time }
        lastState <- state
        if not (changes.IsEmpty) then obs.Next(state, changes)
        reply()
        Angara.MailboxProcessor.AfterMessage.ContinueProcessing state

    let errorHandler (exn:exn) (msg:Message<'v,'d>) state = 
        Trace.StateMachine.TraceEvent(System.Diagnostics.TraceEventType.Critical, 1, sprintf "Execution of the agent results in an exception %O at time %d" exn state.TimeIndex)
        Angara.MailboxProcessor.AfterError.ContinueProcessing state

    member x.Changes = obs.AsObservable
        
    member x.Start() = 
        match agent with
        | Some _ -> invalidOp "StateMachine is already started"
        | None ->
            unsubs <- source.Subscribe(fun message -> 
                match agent with 
                | Some agent -> agent.Post message
                | None -> 
                    Trace.StateMachine.TraceEvent(System.Diagnostics.TraceEventType.Critical, 1, "A message is received before the StateMachine.Start is finished")
                    invalidOp "A message is received before the StateMachine.Start is finished")

            let flowState, changes = normalize initialState
            let state = { Graph = fst initialState; FlowState = flowState; TimeIndex = 1UL }
            lastState <- state
            agent <- Angara.MailboxProcessor.spawnMailboxProcessor (messageHandler, state, errorHandler) |> Option.Some
            obs.Next (state, changes)

        
    member x.State = lastState

    interface System.IDisposable with
        member x.Dispose() = 
            match unsubs with null -> () | d -> d.Dispose()
            agent |> Option.iter(fun d -> d.Dispose())

    static member CreateSuspended (source:System.IObservable<Message<'v, 'd>>) (matchOutput:'d->'d->OutputRef->bool) (initialState:DataFlowGraph<'v> * DataFlowState<'v,VertexState<'d>>) = 
        new StateMachine<'v,'d>(source, initialState, matchOutput) 
