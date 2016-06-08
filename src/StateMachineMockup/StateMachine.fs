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

    type FurtherAction =
    | NoAction
    | DownstreamStartOrIncomplete
    | DownstreamStartOrReproduce
    | DownstreamIncomplete of IncompleteReason
    | UpstreamStartOrReproduce

    let incorrectTransition transition currentStatus =
        failwithf "Incorrect transition '%s' when vertex is in state '%A'" transition currentStatus


    let start currentTime = function
        | VertexStatus.CanStart _ -> VertexStatus.Started currentTime, NoAction
        | VertexStatus.Paused -> VertexStatus.Continues currentTime, NoAction

        | VertexStatus.Paused_MissingOutputOnly -> VertexStatus.Started currentTime, DownstreamIncomplete IncompleteReason.OutdatedInputs
        | VertexStatus.Paused_MissingInputOnly 
        | VertexStatus.Paused_MissingInputOutput -> VertexStatus.Incomplete IncompleteReason.TransientInputs, UpstreamStartOrReproduce        
        | VertexStatus.Final_MissingOutputOnly -> VertexStatus.Reproduces currentTime, NoAction
        | VertexStatus.Final_MissingInputOutput -> VertexStatus.ReproduceRequested, UpstreamStartOrReproduce
        | _ as status -> incorrectTransition "start" status

    let fail exn = function
        | VertexStatus.Started _ 
        | VertexStatus.Continues _
        | VertexStatus.Reproduces _ -> VertexStatus.Incomplete (IncompleteReason.ExecutionFailed exn), NoAction
        | _ as status -> incorrectTransition "fail" status

    let iteration = function
        | VertexStatus.Started t -> VertexStatus.Continues t, DownstreamStartOrIncomplete
        | VertexStatus.Continues t -> VertexStatus.Continues t, DownstreamStartOrIncomplete // todo: filter only changed outputs
        | VertexStatus.Reproduces t -> VertexStatus.Reproduces t, NoAction
        | _ as status -> incorrectTransition "iteration" status

    let succeeded = function
        | VertexStatus.Continues _ -> VertexStatus.Final, NoAction
        | VertexStatus.Reproduces t -> VertexStatus.Final, DownstreamStartOrReproduce
        | _ as status -> incorrectTransition "succeeded" status



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
        

    let makeStartOrIncomplete (state : State) (v : Vertex) : State =
        let makeCanStart state v =
            match vertexStatus state v with
            | VertexStatus.CanStart _ -> state
            | _ ->
                let state' = update state v (VertexStatus.CanStart state.TimeIndex)
                downstreamIncomplete state' v IncompleteReason.OutdatedInputs

        let startTransient (state : State) (v : Vertex) : State = 
            failwith "transient methods not implemented"


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

    let downstreamStartOrIncomplete (state : State) (v : Vertex) : State =
        v |> state.Graph.Structure.OutEdges |> Seq.fold (fun s e -> makeStartOrIncomplete state e.Target) state

    type Message =
    | Start of Vertex
    | Iteration of Vertex
    | Succeeded of Vertex
    | Stop of Vertex
    | Failed of Vertex * exn 
    | Alter
    
    open VertexTransitions

    let transition (m : Message) (state : State) : State =
        let state = { state with TimeIndex = state.TimeIndex  + 1UL }
        let status = vertexStatus state

        // New vertex status
        let v, (vs, action) =
            match m with
            | Start v -> v, status v |> start state.TimeIndex
            | Failed (v, exn) -> v, status v |> fail exn
            | Iteration v -> v, status v |> iteration 
            | Succeeded v -> v, status v |> succeeded 
            | _ as t -> failwithf "transition %A not implemented" t
        let state2 = update state v vs

        // Entry actions:
        let state3 =
            match vs with 
            | VertexStatus.Incomplete _
            | VertexStatus.CanStart _ -> downstreamIncomplete state2 v (IncompleteReason.OutdatedInputs)
            | _ -> state2

        // Transition actions:
        let state4 =
            match action with
            | NoAction -> state3
            | DownstreamIncomplete r -> downstreamIncomplete state3 v r
            | DownstreamStartOrIncomplete -> downstreamStartOrIncomplete state3 v
            | _ -> failwithf "Transition action %A not implemented" action

        state4
        