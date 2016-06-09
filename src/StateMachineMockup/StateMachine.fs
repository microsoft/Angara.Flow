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
    | DownstreamStartOrIncomplete
    | DownstreamStartOrReproduce
    | DownstreamIncomplete of IncompleteReason
    | UpstreamStartOrReproduce

    let incorrectTransition transition currentStatus =
        invalidOp (sprintf "Incorrect transition '%s' when vertex is in state '%A'" transition currentStatus)


    let start currentTime = function
        | VertexStatus.CanStart _ -> VertexStatus.Started currentTime, NoAction
        | VertexStatus.Paused -> VertexStatus.Continues currentTime, NoAction

        | VertexStatus.Paused_MissingOutputOnly -> VertexStatus.Started currentTime, DownstreamIncomplete IncompleteReason.OutdatedInputs
        | VertexStatus.Paused_MissingInputOnly 
        | VertexStatus.Paused_MissingInputOutput -> VertexStatus.Incomplete IncompleteReason.TransientInputs, UpstreamStartOrReproduce        
        | VertexStatus.Final_MissingOutputOnly -> VertexStatus.Reproduces currentTime, NoAction
        | VertexStatus.Final_MissingInputOutput -> VertexStatus.ReproduceRequested, UpstreamStartOrReproduce
        | _ as status -> incorrectTransition "start" status

    let fail exn startTime = function
        | VertexStatus.Started t 
        | VertexStatus.Continues t 
        | VertexStatus.Reproduces t when t = startTime -> VertexStatus.Incomplete (IncompleteReason.ExecutionFailed exn), NoAction
        | VertexStatus.Started _ 
        | VertexStatus.Continues _ 
        | VertexStatus.Reproduces _ as status -> status, NoAction // obsolete message
        | _ as status -> incorrectTransition "fail" status

    let iteration startTime = function
        | VertexStatus.Started t when t = startTime -> VertexStatus.Continues t, DownstreamStartOrIncomplete
        | VertexStatus.Continues t when t = startTime -> VertexStatus.Continues t, DownstreamStartOrIncomplete // todo: filter only changed outputs
        | VertexStatus.Reproduces t when t = startTime -> VertexStatus.Reproduces t, NoAction
        | VertexStatus.Started _
        | VertexStatus.Continues _ 
        | VertexStatus.Reproduces _ as status -> status, NoAction // obsolete message
        | _ as status -> incorrectTransition "iteration" status

    let succeeded startTime = function
        | VertexStatus.Continues t when t = startTime -> VertexStatus.Final, NoAction
        | VertexStatus.Reproduces t when t = startTime -> VertexStatus.Final, DownstreamStartOrReproduce
        | VertexStatus.Continues _
        | VertexStatus.Reproduces _ as status -> status, NoAction // obsolete message
        | _ as status -> incorrectTransition "succeeded" status

    let stop = function
        | VertexStatus.Started t -> VertexStatus.Incomplete IncompleteReason.Stopped, NoAction
        | VertexStatus.Continues t -> VertexStatus.Paused, NoAction
        | _ as status -> incorrectTransition "iteration" status

open VertexTransitions



module StateMachine =
    open Angara.Data

    type State =
        { Graph : DataFlowGraph<Vertex>
          FlowState : Map<Vertex, VertexState> 
          TimeIndex : TimeIndex } 

    let update (state : State) (v : Vertex) (status : VertexStatus) =
        { state with FlowState = state.FlowState |> Map.add v { Status = status } }
    
    let vertexState (state : State) (v : Vertex) =
        state.FlowState |> Map.find v 

    let vertexStatus (state : State) (v : Vertex) =
        (vertexState state v).Status


    type ArtefactStatus = Transient | Uptodate | Outdated
    let getArtefactStatus = function
        | VertexStatus.Incomplete _ 
        | VertexStatus.CanStart _
        | VertexStatus.Started _ -> ArtefactStatus.Outdated

        | VertexStatus.Continues _
        | VertexStatus.Final
        | VertexStatus.Final_MissingInputOnly
        | VertexStatus.Paused 
        | VertexStatus.Paused_MissingInputOnly -> ArtefactStatus.Uptodate

        | VertexStatus.Reproduces _
        | VertexStatus.ReproduceRequested
        | VertexStatus.Final_MissingInputOutput
        | VertexStatus.Final_MissingOutputOnly
        | VertexStatus.Paused_MissingInputOutput
        | VertexStatus.Paused_MissingOutputOnly -> ArtefactStatus.Transient


    let getUpstreamArtefactStatus (state : State) (edge : Edge<Vertex>) : ArtefactStatus option =
        match edge.Type with
        | OneToOne rank when rank = 0 -> getArtefactStatus (vertexStatus state edge.Source) |> Some
        | _ -> failwithf "Not implemented for the edge type %A" edge.Type


    let rec makeIncomplete (state : State) (v : Vertex) reason : State =
        match v |> vertexStatus state with
        | VertexStatus.Incomplete r when r = reason -> 
            state 
        | VertexStatus.Incomplete _
        | VertexStatus.CanStart _
        | VertexStatus.Started _ -> // downstream is already incomplete
            update state v (VertexStatus.Incomplete reason) 
        | VertexStatus.Final
        | VertexStatus.Final_MissingInputOutput
        | VertexStatus.Final_MissingInputOnly
        | VertexStatus.Final_MissingOutputOnly
        | VertexStatus.Paused 
        | VertexStatus.Paused_MissingInputOnly 
        | VertexStatus.Paused_MissingInputOutput
        | VertexStatus.Paused_MissingOutputOnly 
        | VertexStatus.Reproduces _
        | VertexStatus.ReproduceRequested
        | VertexStatus.Continues _ ->
            let state2 = update state v (VertexStatus.Incomplete reason)
            v |> state.Graph.Structure.OutEdges |> Seq.fold (fun s e -> makeIncomplete s e.Target IncompleteReason.OutdatedInputs) state2
    

    let downstreamIncomplete (state : State) (v : Vertex) reason : State =
        v |> state.Graph.Structure.OutEdges |> Seq.fold (fun s e -> makeIncomplete state e.Target reason) state
        

    let rec makeStartOrIncomplete (state : State) (v : Vertex) : State =
        let makeCanStart state v =
            match vertexStatus state v with
            | VertexStatus.CanStart _ -> state
            | _ ->
                let state2 = update state v (VertexStatus.CanStart state.TimeIndex)
                downstreamIncomplete state2 v IncompleteReason.OutdatedInputs

        let startTransient = makeStartOrReproduce 

        let allInputsAssigned (v : Vertex) = 
            let n = v.Inputs.Length
            let inedges = state.Graph.Structure.InEdges v |> Seq.toList
            let is1dArray (t:System.Type) = t.IsArray && t.GetArrayRank() = 1
            seq{ 0 .. (n-1) } |> Seq.forall(fun inRef -> (is1dArray v.Inputs.[inRef]) || inedges |> List.exists(fun e -> e.InputRef = inRef))

        match v.Inputs with 
        | [] -> makeCanStart state v
        | _ when allInputsAssigned v -> 
            let statuses = state.Graph.Structure.InEdges v |> Seq.map(fun e -> e.Source, getUpstreamArtefactStatus state e) 
            let transients, outdated, unassigned = 
                statuses |> Seq.fold(fun (transients, outdated, unassigned) (w, status) ->
                    match status with
                    | Some Transient -> w :: transients, outdated, unassigned
                    | Some Outdated -> transients, true, unassigned
                    | Some Uptodate -> transients, outdated, unassigned
                    | None -> transients, outdated, true
                ) ([], false, false)

            match transients, outdated, unassigned with
            | [], false, false -> makeCanStart state v

            | transients, false, false -> 
                let state2 = makeIncomplete state v IncompleteReason.TransientInputs
                transients 
                |> Seq.distinct 
                |> Seq.fold(fun s t -> startTransient s t) state2

            | _, _, true -> makeIncomplete state v IncompleteReason.UnassignedInputs
            | _, true, _ -> makeIncomplete state v IncompleteReason.OutdatedInputs

        | _ -> // has unassigned inputs
            makeIncomplete state v IncompleteReason.UnassignedInputs

    and downstreamStartOrIncomplete (state : State) (v : Vertex) : State =
        v |> state.Graph.Structure.OutEdges |> Seq.fold (fun s e -> makeStartOrIncomplete state e.Target) state


    and downstreamStartOrReproduce (state : State) (v : Vertex) : State =
        let allInputs w = 
            state.Graph.Structure.InEdges w 
            |> Seq.forall(fun e -> 
                match getUpstreamArtefactStatus state e with
                | Some Transient -> false
                | _ -> true) 

        v |> state.Graph.Structure.OutEdges |> Seq.fold (fun s e -> 
            let w = e.Target
            match vertexStatus state w with
            | VertexStatus.ReproduceRequested -> update state w (VertexStatus.Reproduces state.TimeIndex)
            | VertexStatus.Final_MissingInputOnly when allInputs w -> update state w VertexStatus.Final  
            | VertexStatus.Final_MissingInputOutput when allInputs w -> update state w VertexStatus.Final_MissingOutputOnly  
            
            | VertexStatus.Paused_MissingInputOnly when allInputs w -> update state w VertexStatus.Paused  
            | VertexStatus.Paused_MissingInputOutput when allInputs w -> update state w VertexStatus.Paused_MissingOutputOnly              

            | VertexStatus.Paused 
            | VertexStatus.Paused_MissingOutputOnly
            | VertexStatus.Paused_MissingInputOnly
            | VertexStatus.Paused_MissingInputOutput
            | VertexStatus.Final_MissingInputOnly 
            | VertexStatus.Final_MissingInputOutput            
            | VertexStatus.Final_MissingOutputOnly
            | VertexStatus.Final -> state // no changes

            | _ -> makeStartOrIncomplete state w // try start            
            ) state

    /// Input: `v` is either Final_* or Paused_*
    /// If the vertex has missing output, it is either going to Reproduces, or Started (with downstream invalidated).
    /// If the vertex also has missing input, the function is called recursively for the input.
    and upstreamStartOrReproduce (state : State) (v : Vertex) : State =
        state.Graph.Structure.InEdges v |> Seq.fold(fun s e -> makeStartOrReproduce s e.Source) state

    /// Input: `v` is either Final_* or Paused_*
    /// If the vertex has missing output, it is either going to Reproduces, or Started (with downstream invalidated).
    /// If the vertex also has missing input, the function is called recursively for the input.
    and makeStartOrReproduce (state : State) (v : Vertex) : State =
        let apply (status, effect) = applyTransition state v status effect

        // Since `v` is final or paused, all inputs are assigned and also are either final or paused.
        match vertexStatus state v with
        // These statuses have all inputs and can be started
        | VertexStatus.Final_MissingOutputOnly -> (VertexStatus.Reproduces state.TimeIndex, NoAction) |> apply
        | VertexStatus.Paused_MissingOutputOnly -> (VertexStatus.Started state.TimeIndex, DownstreamIncomplete IncompleteReason.OutdatedInputs) |> apply

        // These statuses has missing inputs and require recursive reproduction
        | VertexStatus.Final_MissingInputOutput -> (VertexStatus.ReproduceRequested, UpstreamStartOrReproduce) |> apply
        | VertexStatus.Paused_MissingInputOutput -> (VertexStatus.Incomplete IncompleteReason.TransientInputs, UpstreamStartOrReproduce) |> apply
        
        // The outputs are presented
        | VertexStatus.Final_MissingInputOnly
        | VertexStatus.Paused_MissingInputOnly 
        | VertexStatus.Reproduces _
        | VertexStatus.ReproduceRequested
        | VertexStatus.Continues _ 
        | VertexStatus.Final
        | VertexStatus.Paused  -> state // nothing to do

        // This status could be if the transient method has multiple inputs and one of them depends on another and it is paused,
        // so its restart makes the other incomplete.
        | VertexStatus.Incomplete _
        | VertexStatus.CanStart _
        | VertexStatus.Started _ -> state // nothing to do, 

    and applyTransition (state : State) (v : Vertex) (targetState : VertexStatus) (effect : TransitionEffect) =
        let state2 = update state v targetState

        // State entry action:
        let state3 =
            match targetState with 
            | VertexStatus.Incomplete _
            | VertexStatus.CanStart _ -> downstreamIncomplete state2 v (IncompleteReason.OutdatedInputs)
            | _ -> state2

        // Transition effect:
        let state4 =
            match effect with
            | NoAction -> state3
            | DownstreamIncomplete r -> downstreamIncomplete state3 v r
            | DownstreamStartOrIncomplete -> downstreamStartOrIncomplete state3 v
            | DownstreamStartOrReproduce -> downstreamStartOrReproduce state3 v
            | UpstreamStartOrReproduce -> upstreamStartOrReproduce state3 v

        state4

    type Response<'a> =
        | Success       of 'a
        | Exception     of System.Exception
    type ReplyChannel<'a> = Response<'a> -> unit

    type Message =
    | Start of Vertex
    | Iteration of Vertex * startTime:TimeIndex
    | Succeeded of Vertex * startTime:TimeIndex
    | Stop of Vertex
    | Failed of Vertex * exn * startTime:TimeIndex 
    | Alter
    
    open VertexTransitions

    let transition (m : Message) (state : State) : State =
        let state = { state with TimeIndex = state.TimeIndex  + 1UL }
        let status = vertexStatus state

        let v, (vs, transitionEffect) =
            match m with
            | Start v -> v, status v |> start state.TimeIndex
            | Failed (v, exn, startTime) -> v, status v |> fail exn startTime
            | Iteration (v, startTime) -> v, status v |> iteration startTime
            | Succeeded (v, startTime) -> v, status v |> succeeded startTime
            | Stop v -> v, status v |> stop
            | Alter -> failwithf "transition Alter not implemented"
        
        let state2 = applyTransition state v vs transitionEffect
        
        state2
