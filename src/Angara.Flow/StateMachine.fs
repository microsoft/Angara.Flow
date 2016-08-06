namespace Angara.States

open Angara.Graph

module Messages =
    open TransitionEffects

    type Response<'a> =
        | Success       of 'a
        | Exception     of System.Exception
    type ReplyChannel<'a> = Response<'a> -> unit

    type StartMessage<'v> =
        { Vertex: 'v
          Index: VertexIndex
          CanStartTime: TimeIndex option }

    type SucceededMessage<'v> =
        { Vertex: 'v
          Index: VertexIndex          
          StartTime: TimeIndex }

    type IterationMessage<'v> =
        { Vertex: 'v
          Index: VertexIndex          
          Result: IVertexData
          StartTime: TimeIndex }

    type FailedMessage<'v> =
        { Vertex: 'v
          Index: VertexIndex
          Failure: exn
          StartTime: TimeIndex }

    type StopMessage<'v> =
        { Vertex: 'v
          Index: VertexIndex }

    type Message<'v when 'v:comparison and 'v:>IVertex> =
        | Start         of StartMessage<'v> //* ReplyChannel<bool>
        | Stop          of StopMessage<'v>
        | Iteration     of IterationMessage<'v>
        | Succeeded     of SucceededMessage<'v> // todo: hot/cold final artefacts (incl. distributed case, June/July 2016)
        | Failed        of FailedMessage<'v>

    open VertexTransitions

    let noChanges = Map.empty

    let transition (m : Message<'v>) (state : State<'v>) : StateUpdate<'v> =
        let state = { state with TimeIndex = state.TimeIndex  + 1UL }
        
        let vertexState = vertexState state 
        let changeStatus vs (status,effect:TransitionEffect) = { vs with Status = status }, effect

        let vi, (vs, transitionEffect) =
            match m with
            | Start s -> 
                let v = s.Vertex,s.Index
                let vs = vertexState v                   
                v, start state.TimeIndex vs.Status |> changeStatus vs

            | Failed f ->
                let v = f.Vertex,f.Index
                let vs = vertexState v                   
                v, fail f.Failure f.StartTime vs.Status |> changeStatus vs

            | Iteration it -> 
                let v = it.Vertex,it.Index
                let vs = vertexState v                   
                v, iteration it.StartTime it.Result.Shape vs.Status |> changeStatus vs

            | Succeeded succ -> 
                let v = succ.Vertex,succ.Index
                let vs = vertexState v                   
                v, succeeded succ.StartTime vs.Status |> changeStatus vs

            | Stop st -> 
                let v = st.Vertex,st.Index
                let vs = vertexState v                   
                v, stop vs.Status |> changeStatus vs
        
        let state2 = applyTransition { State = state; Changes = noChanges } vi vs transitionEffect
        
        state2

open System
open Angara
open Messages

// TODO:
// 0. Message Start should reply.
// 1. Match outputs: if next iteration produces identical artefact, it doesn't outdate the corresponding dependencies.
// 2. Transiency must be property of individual outputs, not entire method as it is done now.
//      Possible solution: states Final and Paused have boolean vectors for outputs (inputs?) indicating what is presented/missing.
// 3. Add Alter operations.
// 4. Add cold/hot artefacts, thus enable distributed case (see paper notes for June/July 2016).
[<Sealed>]
type StateMachine<'v when 'v:comparison and 'v:>IVertex> private (initialState : State<'v>, source : IObservable<Message<'v>>) =

    let obs = Angara.Observable.ObservableSource<StateUpdate<'v>>()
    let mutable agent : Angara.MailboxProcessor.ILinearizingAgent<Message<'v>> option = None
    let mutable unsubs : System.IDisposable = null
    let mutable lastState = initialState
        
    let messageHandler (msg:Message<'v>) (state:State<'v>) = 
        let update(*, reply*) = transition msg state
        lastState <- update.State
        if not (update.Changes.IsEmpty) then obs.Next update
        //reply()
        Angara.MailboxProcessor.AfterMessage.ContinueProcessing state

    let errorHandler (exn:exn) (msg:Message<'v>) state = 
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

            let update = normalize initialState
            lastState <- update.State
            agent <- Angara.MailboxProcessor.spawnMailboxProcessor (messageHandler, update.State, errorHandler) |> Option.Some
            if not(update.Changes.IsEmpty) then obs.Next update

        
    member x.State = lastState

    interface System.IDisposable with
        member x.Dispose() = 
            match unsubs with null -> () | d -> d.Dispose()
            agent |> Option.iter(fun d -> d.Dispose())

    static member CreateSuspended (source:System.IObservable<Message<'v>>) (initialState:State<'v>) = 
        new StateMachine<'v>(initialState, source) 
