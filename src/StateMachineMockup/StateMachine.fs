namespace StateMachineMockup

// https://fsharpforfunandprofit.com/posts/designing-with-types-representing-states/

open System
open Angara.Graph
open Vertices

type Vertex(name : string, inputs, outputs) = 
    
    member x.Name = name
    member x.Inputs : System.Type list = inputs
    member x.Outputs : System.Type list = outputs

    interface IVertex with
        member x.Inputs : System.Type list = inputs
        member x.Outputs : System.Type list = outputs

    interface IComparable with
        member x.CompareTo(other : obj) =
            match other with
            | null -> 1
            | :? Vertex as v -> name.CompareTo v.Name
            | _ -> raise (new ArgumentException("Object is not a Vertex"))

    override x.ToString() = sprintf "Vertex '%s'" name




module VertexTransitions =

    type TransitionEffect =
    | NoAction
    | DownstreamStartOrIncomplete of shapeChanged: bool
    | DownstreamStartOrReproduce
    | DownstreamIncomplete of IncompleteReason
    | UpstreamStartOrReproduce

    let incorrectTransition transition currentStatus =
        invalidOp (sprintf "Incorrect transition '%s' when vertex is in state '%A'" transition currentStatus)


    let start currentTime = function
        | VertexStatus.CanStart _ -> VertexStatus.Started currentTime, NoAction
        | VertexStatus.Paused shape -> VertexStatus.Continues (currentTime, shape), NoAction

        | VertexStatus.Paused_MissingOutputOnly _ -> VertexStatus.Started currentTime, DownstreamIncomplete IncompleteReason.OutdatedInputs
        | VertexStatus.Paused_MissingInputOnly _ 
        | VertexStatus.Paused_MissingInputOutput _ -> VertexStatus.Incomplete IncompleteReason.TransientInputs, UpstreamStartOrReproduce        
        | VertexStatus.Final_MissingOutputOnly shape -> VertexStatus.Reproduces (currentTime, shape), NoAction
        | VertexStatus.Final_MissingInputOutput shape -> VertexStatus.ReproduceRequested shape, UpstreamStartOrReproduce
        | _ as status -> incorrectTransition "start" status

    let fail exn startTime = function
        | VertexStatus.Started t 
        | VertexStatus.Continues (t,_) 
        | VertexStatus.Reproduces (t,_) when t = startTime -> VertexStatus.Incomplete (IncompleteReason.ExecutionFailed exn), NoAction
        | VertexStatus.Started _ 
        | VertexStatus.Continues _ 
        | VertexStatus.Reproduces _ as status -> status, NoAction // obsolete message
        | _ as status -> incorrectTransition "fail" status

    let iteration startTime shape = function
        | VertexStatus.Started t when t = startTime -> VertexStatus.Continues (t,shape), DownstreamStartOrIncomplete true
        | VertexStatus.Continues (t,oldShape) when t = startTime -> VertexStatus.Continues (t, shape), DownstreamStartOrIncomplete (shape <> oldShape)  // todo: filter only changed outputs
        | VertexStatus.Reproduces (t,oldShape) when t = startTime -> VertexStatus.Reproduces (t,oldShape), NoAction
        | VertexStatus.Started _
        | VertexStatus.Continues _ 
        | VertexStatus.Reproduces _ as status -> status, NoAction // obsolete message
        | _ as status -> incorrectTransition "iteration" status

    let succeeded startTime = function
        | VertexStatus.Continues (t,shape) when t = startTime -> VertexStatus.Final shape, NoAction
        | VertexStatus.Reproduces (t,shape) when t = startTime -> VertexStatus.Final shape, DownstreamStartOrReproduce
        | VertexStatus.Continues _
        | VertexStatus.Reproduces _ as status -> status, NoAction // obsolete message
        | _ as status -> incorrectTransition "succeeded" status

    let stop = function
        | VertexStatus.Started t -> VertexStatus.Incomplete IncompleteReason.Stopped, NoAction
        | VertexStatus.Continues (t,shape) -> VertexStatus.Paused shape, NoAction
        | _ as status -> incorrectTransition "iteration" status

open VertexTransitions



module StateMachine =
    open Angara.Data
    open Angara.Option
    open Angara

    type State =
        { Graph : DataFlowGraph<Vertex>
          FlowState : Map<Vertex, MdMap<int,VertexState>> 
          TimeIndex : TimeIndex } 

    type VertexItem = Vertex * VertexIndex

    let vertexState (state : State) (v : Vertex, i : VertexIndex) =
        state.FlowState |> Map.find v |> MdMap.tryFind i |> Option.get

    let tryVertexState (state : State) (v : Vertex, i : VertexIndex) =
        opt {
            let! vs = state.FlowState |> Map.tryFind v 
            return! MdMap.tryFind i vs
        }

    let vertexStatus (state : State) (vi : VertexItem) =
        (vertexState state vi).Status

    let tryVertexStatus (state : State) (vi : VertexItem) =
        opt { 
            let! vs = tryVertexState state vi
            return vs.Status
        }


    let update (state : State) (v : Vertex, i : VertexIndex) (vsi : VertexState) =
        let vs = state.FlowState |> Map.find v
        let nvs = vs |> MdMap.add i vsi
        { state with FlowState = state.FlowState |> Map.add v nvs }

    let replace (state : State) (v : Vertex) (vs : MdMap<int,VertexState>) =
        { state with FlowState = state.FlowState |> Map.add v vs }
    
    
    /// Returns a sequence of vertex items that are immediate downstream vertex items which are represented in the flow state.
    let enumerateDownstream (state : State) (v : Vertex, i : VertexIndex) : VertexItem seq =
        state.Graph.Structure.OutEdges v
        |> Seq.map (fun e -> 
            let j = 
                match e.Type with
                | OneToOne _ | Scatter _ | Collect _ -> i // actual indices will be equal or longer
                | Reduce _ -> List.removeLast i
            match state.FlowState.TryFind e.Target with // finds real presented indices
            | Some tvs -> tvs |> MdMap.startingWith j |> MdMap.toSeq |> Seq.map(fun (k,_) -> e.Target,k)
            | None -> Seq.empty)
        |> Seq.concat

    let enumerateUpstream (state : State) (v : Vertex, i : VertexIndex) : VertexItem seq =
        let rec minIndex ws j =
            match j with
            | [] -> []
            | j1 :: jr ->
                match ws |> MdMap.tryGet [j1] with
                | Some ws2 -> j1 :: (minIndex ws2 jr)
                | None -> []
        state.Graph.Structure.InEdges v
        |> Seq.map (fun e ->
            let w = e.Source
            match e.Type with
            | OneToOne rank -> seq { yield w, List.ofMaxLength rank i }
            | Scatter rank when i.Length >= rank -> seq { yield w, List.ofMaxLength (rank-1) i }
            | Scatter rank -> // e.g. input of `v` is unassigned
                seq { yield w, List.ofMaxLength (rank-1) i |> minIndex (state.FlowState  |> Map.find w)  } 
            | Reduce rank ->
                let j = List.ofMaxLength rank i
                let ws = state.FlowState |> Map.find w
                let rs_ind = 
                    ws
                    |> MdMap.startingWith j
                    |> MdMap.toSeq 
                    |> Seq.map fst 
                    |> Seq.toList
                match rs_ind with
                | []  -> seq { yield w, minIndex ws j } 
                | _ -> rs_ind |> Seq.map(fun idx -> w, idx)
            | Collect (_,rank) -> seq { yield w, List.ofMaxLength rank i })
        |> Seq.concat

    type ArtefactStatus = Transient | Uptodate | Outdated
    let getArtefactStatus = function
        | VertexStatus.Incomplete _ 
        | VertexStatus.CanStart _
        | VertexStatus.Started _ -> ArtefactStatus.Outdated

        | VertexStatus.Continues _
        | VertexStatus.Final _
        | VertexStatus.Final_MissingInputOnly _
        | VertexStatus.Paused _
        | VertexStatus.Paused_MissingInputOnly _ -> ArtefactStatus.Uptodate

        | VertexStatus.Reproduces _
        | VertexStatus.ReproduceRequested _
        | VertexStatus.Final_MissingInputOutput _
        | VertexStatus.Final_MissingOutputOnly _
        | VertexStatus.Paused_MissingInputOutput _
        | VertexStatus.Paused_MissingOutputOnly _ -> ArtefactStatus.Transient


    let getUpstreamArtefactStatus (state : State) (vi : VertexItem) : (VertexItem * ArtefactStatus option) seq =
        enumerateUpstream state vi
        |> Seq.map(fun wj -> wj, Some(vertexStatus state wj |> getArtefactStatus)) 

    let rec makeIncomplete (state : State) (v : Vertex, i : VertexIndex) reason : State =
        match (v,i) |> tryVertexState state with
        | None -> state 
        | Some vs ->
            match vs.Status with
            | VertexStatus.Incomplete r when r = reason -> 
                state 
            | VertexStatus.Incomplete _
            | VertexStatus.CanStart _
            | VertexStatus.Started _ -> // downstream is already incomplete
                update state (v,i) { vs with Status = VertexStatus.Incomplete reason }
            | VertexStatus.Final _
            | VertexStatus.Final_MissingInputOutput _
            | VertexStatus.Final_MissingInputOnly _
            | VertexStatus.Final_MissingOutputOnly _
            | VertexStatus.Paused _
            | VertexStatus.Paused_MissingInputOnly _
            | VertexStatus.Paused_MissingInputOutput _
            | VertexStatus.Paused_MissingOutputOnly _
            | VertexStatus.Reproduces _
            | VertexStatus.ReproduceRequested _
            | VertexStatus.Continues _ ->
                let state2 = update state (v,i) { vs with Status = VertexStatus.Incomplete reason }
                downstreamIncomplete state2 (v,i) IncompleteReason.OutdatedInputs    

    and downstreamIncomplete (state : State) (v : Vertex, i : VertexIndex) reason : State =
        enumerateDownstream state (v,i)
        |> Seq.fold (fun s wj -> makeIncomplete s wj reason) state

    
    let allInputsAssigned (state : State) (v : Vertex) = 
        let n = v.Inputs.Length
        let inedges = state.Graph.Structure.InEdges v |> Seq.toList
        let is1dArray (t:System.Type) = t.IsArray && t.GetArrayRank() = 1
        seq{ 0 .. (n-1) } |> Seq.forall(fun inRef -> (is1dArray v.Inputs.[inRef]) || inedges |> List.exists(fun e -> e.InputRef = inRef))

    /// When 'vi' succeeds and thus we have its output artefacts, this method updates downstream vertices so their state dimensionality corresponds to the given output.
    let downstreamShape (state : State) (vi : VertexItem) : State =
        let outdatedItems n item = Seq.init n id |> Seq.fold(fun ws j -> ws |> MdMap.add [j] (item j)) MdMap.empty

        let dag = state.Graph.Structure
        let v,i = vi
        let w = dag.OutEdges v |> Seq.choose(fun e -> match e.Type with Scatter _ -> Some e.Target | _ -> None) 
        let r = vertexRank v dag
        let scope = Graph.toSeqSubgraph (fun u _ _ -> (vertexRank u dag) <= r) dag w |> Graph.topoSort dag
        scope |> Seq.fold(fun (state : State) w ->
            match w |> allInputsAssigned state with
            | false -> // some inputs are unassigned
                match (w,[]) |> tryVertexState state with
                | Some wis when wis.Status.IsIncompleteReason IncompleteReason.UnassignedInputs -> state
                | Some wis -> replace state w (MdMap.scalar { wis with Status = Incomplete UnassignedInputs })
                | None -> replace state w (MdMap.scalar VertexState.Unassigned)
            | true -> // all inputs are assigned
                let getDimLength u (edgeType:ConnectionType) (outRef: OutputRef) : int option =
                    let urank = vertexRank u dag
                    if urank < r then None // doesn't depend on the target dimension
                    else if urank = r then
                        match edgeType with
                        | Scatter _ -> 
                            opt {
                                let! us = state.FlowState |> Map.tryFind u
                                let! uis = us |> MdMap.tryFind i
                                let! shape = uis.Status.TryGetShape()
                                return shape.[outRef]
                            }
                        | _ -> None // doesn't depend on the target dimension
                    else // urank > r; u is vectorized for the dimension
                        match state.FlowState.[u] |> MdMap.tryGet i with
                        | Some selection when not selection.IsScalar -> Some(selection |> MdMap.toShallowSeq |> Seq.length)
                        | _ -> None
                
                let ws = state.FlowState.[w]
                match ws |> MdMap.tryGet i with
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
                        let wis = 
                            match dimLen with
                            | Some (minDim,maxDim) -> outdatedItems maxDim (fun j -> if j < minDim then VertexState.Outdated else VertexState.Unassigned)
                            | None -> failwith "Unexpected case: the vertex should be downstream of the succeeded vector vertex"
                        replace state w (ws |> MdMap.set i wis)
                    | None -> state // doesn't depend on the target dimension
            ) state
       

    let rec makeStartOrIncomplete (state : State) (vi : VertexItem) : State =
        let makeCanStart state vi =
            let vs = vertexState state vi
            match vs.Status with
            | VertexStatus.CanStart _ -> state
            | _ ->
                let state2 = update state vi { vs with Status = VertexStatus.CanStart state.TimeIndex }
                downstreamIncomplete state2 vi IncompleteReason.OutdatedInputs

        let startTransient = makeStartOrReproduce 

        let v,i = vi
        match v.Inputs with 
        | [] -> makeCanStart state vi
        | _ when allInputsAssigned state v -> 
            let statuses = getUpstreamArtefactStatus state vi
            let transients, outdated, unassigned = 
                statuses |> Seq.fold(fun (transients, outdated, unassigned) (w, status) ->
                    match status with
                    | Some Transient -> w :: transients, outdated, unassigned
                    | Some Outdated -> transients, true, unassigned
                    | Some Uptodate -> transients, outdated, unassigned
                    | None -> transients, outdated, true
                ) ([], false, false)

            match transients, outdated, unassigned with
            | [], false, false -> makeCanStart state vi

            | transients, false, false -> 
                let state2 = makeIncomplete state vi IncompleteReason.TransientInputs
                transients 
                |> Seq.distinct 
                |> Seq.fold(fun s t -> startTransient s t) state2

            | _, _, true -> makeIncomplete state vi IncompleteReason.UnassignedInputs
            | _, true, _ -> makeIncomplete state vi IncompleteReason.OutdatedInputs

        | _ -> // has unassigned inputs
            makeIncomplete state vi IncompleteReason.UnassignedInputs

    and downstreamStartOrIncomplete (state : State) (vi : VertexItem) : State = 
        enumerateDownstream state vi
        |> Seq.fold (fun s wj -> makeStartOrIncomplete s wj) state


    and downstreamStartOrReproduce (state : State) (vi : VertexItem) : State =
        let allInputs wj = 
            getUpstreamArtefactStatus state wj
            |> Seq.forall(fun (_, status) -> match status with Some Transient -> false | _ -> true)

        enumerateDownstream state vi
        |> Seq.fold (fun s wj -> 
            let ws = vertexState state wj
            match ws.Status with
            | VertexStatus.ReproduceRequested shape -> update state wj { ws with Status = VertexStatus.Reproduces (state.TimeIndex, shape) }
            | VertexStatus.Final_MissingInputOnly shape when allInputs wj -> update state wj { ws with Status = VertexStatus.Final shape }
            | VertexStatus.Final_MissingInputOutput shape when allInputs wj -> update state wj { ws with Status = VertexStatus.Final_MissingOutputOnly shape } 
            
            | VertexStatus.Paused_MissingInputOnly shape when allInputs wj -> update state wj { ws with Status = VertexStatus.Paused shape } 
            | VertexStatus.Paused_MissingInputOutput shape when allInputs wj -> update state wj { ws with Status = VertexStatus.Paused_MissingOutputOnly shape }             

            | VertexStatus.Paused _ 
            | VertexStatus.Paused_MissingOutputOnly _
            | VertexStatus.Paused_MissingInputOnly _
            | VertexStatus.Paused_MissingInputOutput _
            | VertexStatus.Final_MissingInputOnly _
            | VertexStatus.Final_MissingInputOutput _            
            | VertexStatus.Final_MissingOutputOnly _
            | VertexStatus.Final _ -> state // no changes

            | _ -> makeStartOrIncomplete state wj // try start            
            ) state

    /// Input: `v` is either Final_* or Paused_*
    /// If the vertex has missing output, it is either going to Reproduces, or Started (with downstream invalidated).
    /// If the vertex also has missing input, the function is called recursively for the input.
    and upstreamStartOrReproduce (state : State) (vi : VertexItem) : State =
        enumerateUpstream state vi |> Seq.fold(fun s wj -> makeStartOrReproduce s wj) state

    /// Input: `v` is either Final_* or Paused_*
    /// If the vertex has missing output, it is either going to Reproduces, or Started (with downstream invalidated).
    /// If the vertex also has missing input, the function is called recursively for the input.
    and makeStartOrReproduce (state : State) (vi : VertexItem) : State =
        let vs = vertexState state vi
        let apply (status, effect) = applyTransition state vi (vs.NewStatus status) effect

        // Since `v` is final or paused, all inputs are assigned and also are either final or paused.
        match vs.Status with
        // These statuses have all inputs and can be started
        | VertexStatus.Final_MissingOutputOnly shape -> (VertexStatus.Reproduces (state.TimeIndex,shape), NoAction) |> apply
        | VertexStatus.Paused_MissingOutputOnly _ -> (VertexStatus.Started state.TimeIndex, DownstreamIncomplete IncompleteReason.OutdatedInputs) |> apply

        // These statuses has missing inputs and require recursive reproduction
        | VertexStatus.Final_MissingInputOutput shape -> (VertexStatus.ReproduceRequested shape, UpstreamStartOrReproduce) |> apply
        | VertexStatus.Paused_MissingInputOutput _ -> (VertexStatus.Incomplete IncompleteReason.TransientInputs, UpstreamStartOrReproduce) |> apply
        
        // The outputs are presented
        | VertexStatus.Final_MissingInputOnly _
        | VertexStatus.Paused_MissingInputOnly _
        | VertexStatus.Reproduces _
        | VertexStatus.ReproduceRequested _
        | VertexStatus.Continues _ 
        | VertexStatus.Final _
        | VertexStatus.Paused _  -> state // nothing to do

        // This status could be if the transient method has multiple inputs and one of them depends on another and it is paused,
        // so its restart makes the other incomplete.
        | VertexStatus.Incomplete _
        | VertexStatus.CanStart _
        | VertexStatus.Started _ -> state // nothing to do, 

    and applyTransition (state : State) (vi : VertexItem) (targetState : VertexState) (effect : TransitionEffect) =
        let state2 = update state vi targetState

        // State entry action:
        let state3 =
            match targetState.Status with 
            | VertexStatus.Incomplete _
            | VertexStatus.CanStart _ -> downstreamIncomplete state2 vi (IncompleteReason.OutdatedInputs)
            | _ -> state2

        // Transition effect:
        let state4 =
            match effect with
            | NoAction -> state3
            | DownstreamIncomplete r -> downstreamIncomplete state3 vi r
            | DownstreamStartOrIncomplete shapeChanged -> 
                let s = if shapeChanged then downstreamShape state3 vi else state3
                downstreamStartOrIncomplete s vi
            | DownstreamStartOrReproduce -> downstreamStartOrReproduce state3 vi
            | UpstreamStartOrReproduce -> upstreamStartOrReproduce state3 vi

        state4

    type Response<'a> =
        | Success       of 'a
        | Exception     of System.Exception
    type ReplyChannel<'a> = Response<'a> -> unit

    type Message =
    | Start of Vertex * VertexIndex
    | Iteration of Vertex * VertexIndex * shape:int list * startTime:TimeIndex
    | Succeeded of Vertex * VertexIndex * startTime:TimeIndex
    | Stop of Vertex * VertexIndex
    | Failed of Vertex * VertexIndex * exn * startTime:TimeIndex 
    | Alter
    
    open VertexTransitions

    let transition (m : Message) (state : State) : State =
        let state = { state with TimeIndex = state.TimeIndex  + 1UL }
        
        let status = vertexStatus state 
        let ofStatus (status,effect) = { Status = status }, effect

        let vi, (vs, transitionEffect) =
            match m with
            | Start (v,i) -> (v,i), status (v,i) |> start state.TimeIndex |> ofStatus
            | Failed (v,i,exn,startTime) -> (v,i), status (v,i) |> fail exn startTime |> ofStatus
            | Iteration (v,i,shape,startTime) -> (v,i), status (v,i) |> iteration startTime shape |> ofStatus
            | Succeeded (v,i,startTime) -> (v,i), status (v,i) |> succeeded startTime |> ofStatus
            | Stop (v,i) -> (v,i), status (v,i) |> stop |> ofStatus
            | Alter -> failwithf "transition Alter not implemented"
        
        let state2 = applyTransition state vi vs transitionEffect
        
        state2
