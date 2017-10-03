namespace Angara.Execution

open System
open System.Threading
open Angara.Graph
open Angara.RuntimeContext
open Angara.Option
open Angara.States

type Artefact = obj
type MethodCheckpoint = obj
type MethodId = Guid

type OutputArtefacts = 
    | Full    of Artefact list
    | Partial of (Artefact option) list
    member x.TryGet (outRef:OutputRef) =
        match x with
        | Full artefacts -> Some(artefacts.[outRef])
        | Partial artefacts -> if outRef < artefacts.Length then artefacts.[outRef] else None

[<AbstractClass>]
type ExecutableMethod(id: MethodId, inputs: Type list, outputs: Type list) =

    interface IVertex with
        member x.Inputs = inputs
        member x.Outputs = outputs
    
    interface IComparable with
        member x.CompareTo(yobj: obj): int = 
            match yobj with
            | :? ExecutableMethod as y -> id.CompareTo y.Id
            | _ -> invalidArg "yobj" "Cannot compare values of different types"

    member x.Id = id

    override x.Equals(yobj: obj) = 
        match yobj with
        | :? ExecutableMethod as y -> x.Id = y.Id
        | _ -> false

    override x.GetHashCode() = 
        id.GetHashCode()
                
    abstract Execute : Artefact list * MethodCheckpoint option -> (Artefact list * MethodCheckpoint) seq
       

[<Class>] 
type MethodOutput private (shape: OutputShape, output: OutputArtefacts, checkpoint: MethodCheckpoint option) =
    
    static let getShape (a:Artefact) = 
        match a with
        | :? Array as a when a.Rank = 1 -> a.Length
        | _ -> 0


    interface IVertexData with
        member x.Shape = shape
        
    member x.Artefacts = output
    member x.Checkpoint = checkpoint

    member x.TryGet(outRef) : Artefact option = 
        match output with
        | OutputArtefacts.Full art when outRef < art.Length -> art.[outRef] |> Some
        | OutputArtefacts.Partial art when outRef < art.Length -> art.[outRef]
        | _ -> None
    
    static member Full(artefacts: Artefact list, checkpoint: MethodCheckpoint option) =
        MethodOutput(artefacts |> List.map getShape, OutputArtefacts.Full artefacts, checkpoint)

 
type Input = 
    | NotAvailable
    | Item of Artefact
    | Array of Artefact array 

module Artefacts =
    open Angara.Option
    open Angara.Data
    
    let internal tryGetOutput (edge:Edge<'m>) (i:VertexIndex) (state:VerticesState<'m,VertexState<MethodOutput>>) : Artefact option =
        opt {
            let! vs = state |> Map.tryFind edge.Source
            let! vis = vs |> MdMap.tryFind i
            let! data = vis.Data
            return! data.Artefacts.TryGet edge.OutputRef
        }

    /// Sames as getOutput, but "i" has rank one less than rank of the source vertex,
    /// therefore result is an array of artefacts for all available indices complementing "i".
    let internal tryGetReducedOutput (edge:Edge<_>) (i:VertexIndex) (state:VerticesState<'v,VertexState<MethodOutput>>) : Artefact[] option =
        match state |> Map.tryFind edge.Source with
        | Some svs ->
            let r = i.Length
            let items = 
                svs 
                |> MdMap.startingWith i 
                |> MdMap.toSeq 
                |> Seq.filter(fun (j,_) -> j.Length = r + 1) 
                |> Seq.map(fun (j,vis) -> (List.last j, vis.Data |> Option.bind(fun data -> data.Artefacts.TryGet edge.OutputRef))) 
                |> Seq.toList
            match items with
            | [] -> Some Array.empty
            | _ ->
                let n = items |> Seq.map fst |> Seq.max
                let map = items |> Map.ofList
                let artefacts = Seq.init (n+1) (fun i -> i) |> Seq.map(fun i -> match map.TryFind(i) with | Some(a) -> a | None -> None) |> Seq.toList
                if artefacts |> List.forall Option.isSome then artefacts |> Seq.map Option.get |> Seq.toArray |> Option.Some
                else None
        | None -> None

    /// Returns the vertex' output artefact as n-dimensional typed jagged array, where n is a rank of the vertex.
    /// If n is zero, returns the typed object itself.
    let getMdOutput (v:'v) (outRef: OutputRef) (graph:FlowGraph<'v>, state:VerticesState<'v,VertexState<MethodOutput>>) = 
        let vector = state |> Map.find v |> MdMap.map (fun vis -> vis.Data.Value.Artefacts.TryGet(outRef).Value)
        let rank = vertexRank v graph.Structure
        if rank = 0 then 
            vector |> MdMap.find []
        else 
            let objArr = MdMap.toJaggedArray vector
            let artType = v.Outputs.[outRef]
            let arrTypes = Seq.unfold(fun (r : int, t : System.Type) ->
                if r > 0 then Some(t, (r-1, t.MakeArrayType())) else None) (rank, artType) |> Seq.toList

            let rec cast (eltType:System.Type) (r:int) (arr:obj[]) : System.Array =
                let n = arr.Length
                if r = 1 then
                    let tarr = System.Array.CreateInstance(eltType, n)
                    if n > 0 then arr.CopyTo(tarr, 0)
                    tarr
                else             
                    let tarr = System.Array.CreateInstance(arrTypes.[r-1], n)
                    for i = 0 to n-1 do tarr.SetValue(cast eltType (r-1) (arr.[i]:?>obj[]), i)
                    tarr
            upcast cast artType rank objArr

    /// <summary>Collects inputs for the vertex slice; represents the reduced input as an array.</summary>
    /// <returns>An array of input snapshots, element of an array corresponds to the respective input port.
    /// If element is NotAvailable, no artefact(s) exists for that input in the state.
    /// If element is Item, there is a single artefact for the input.
    /// If element is Array, there are a number of artefacts that altogether is an input (i.e. `reduce` or `collect` input edges).
    /// </returns>
    let getInputs (state: VerticesState<'m,VertexState<_>>, graph: FlowGraph<'m>) (v:'m, i:VertexIndex) : Input[] =
        let inputTypes = (v :> IVertex).Inputs
        let inputs = Array.init inputTypes.Length (fun i -> if inputTypes.[i].IsArray then Input.Array (Array.empty) else Input.NotAvailable)
        graph.Structure.InEdges v
        |> Seq.groupBy (fun e -> e.InputRef)
        |> Seq.iter (fun (inRef, edges) -> 
               let reduceToEdgeIndex (edge:Edge<'v>) = List.ofMaxLength (edgeRank edge)

               match List.ofSeq edges with
               | [] -> inputs.[inRef] <- Input.NotAvailable

               | [edge] -> // single input edge
                    let edgeIndex = i |> reduceToEdgeIndex edge
                    match edge.Type with
                    | Collect (_,_) -> 
                        match tryGetOutput edge edgeIndex state with
                        | None -> inputs.[inRef] <- Input.NotAvailable
                        | Some(a) -> inputs.[inRef] <- Input.Array [| a |]

                    | Scatter _ ->
                        match tryGetOutput edge (edgeIndex |> List.removeLast) state with
                        | None -> inputs.[inRef] <- Input.NotAvailable
                        | Some(a) -> inputs.[inRef] <- Input.Item ((a:?>System.Array).GetValue(List.last edgeIndex))

                    | Reduce _ ->
                        match tryGetReducedOutput edge edgeIndex state with
                        | None -> inputs.[inRef] <- Input.NotAvailable
                        | Some(a) -> inputs.[inRef] <- Input.Array a

                    | OneToOne _ -> 
                        match tryGetOutput edge edgeIndex state with
                        | None -> inputs.[inRef] <- Input.NotAvailable
                        | Some(a) -> inputs.[inRef] <- Input.Item a

               | edges -> // All multiple input edges have type "Collect" due to type check on connection
                   let index (e : IEdge<'v>) = 
                       match (e :?> Edge<'v>).Type with
                       | Collect (idx,_) -> idx
                       | _ -> failwith "Expecting 'Collect' input edge but the edge has different type"
               
                   let artefacts = 
                       edges
                       |> Seq.map (fun e -> 
                             let edgeIndex = i |> reduceToEdgeIndex e
                             index e, tryGetOutput e edgeIndex state)
                       |> Array.ofSeq
               
                   if artefacts |> Seq.forall (fun (_, a) -> Option.isSome a) then 
                       let artefacts = 
                           artefacts
                           |> Seq.sortBy fst
                           |> Seq.map (snd >> Option.get)
                           |> Array.ofSeq
                       inputs.[inRef] <- Input.Array(artefacts)
                   else inputs.[inRef] <- Input.NotAvailable)
        inputs 

//////////////////////////////////////////////
// 
// Changes Analysis
//
//////////////////////////////////////////////

type RuntimeAction<'v> =
    | Delay     of 'v * VertexIndex * TimeIndex
    | Execute   of 'v * VertexIndex * TimeIndex 
    | Continue  of 'v * VertexIndex * TimeIndex 
    | StopMethod of 'v * VertexIndex * TimeIndex
    | Remove    of 'v

module Analysis =
    open Angara
    open Angara.Data

    /// Analyzes state machine changes and provides a list of action to be performed by an execution runtime.
    /// All methods are executed when have status "CanStart" or "Reproduces".
    let internal analyzeChanges (update: StateUpdate<'v,'d>) : RuntimeAction<'v> list =
        let processItemChange (v: 'v, i:VertexIndex) (oldvis: VertexState<'d> option) (vis: VertexState<'d>) : RuntimeAction<'v> option =
            let oldStatus = 
                match oldvis with
                | Some oldvis -> oldvis.Status
                | None -> Incomplete IncompleteReason.UnassignedInputs
    
            match oldStatus, vis.Status with
            // -- Delay
            | CanStart t1, CanStart t2 when t1 <> t2 -> Delay (v,i,t2) |> Some
            | _, CanStart t -> Delay (v,i,t) |> Some
                
            // -- Execute from beginning
            | CanStart _, Started t
            | _, Reproduces (t,_) 
            | Paused_MissingOutputOnly _, Started t -> Execute (v,i,t) |> Some
    
            // -- Continue from checkpoint
            | Paused _, Continues (t,_) -> Continue (v,i,t) |> Some
                
            // -- Stop execution
            | Continues (t,_), Paused _     // stops iterations
            | Started t, Incomplete (IncompleteReason.Stopped) -> StopMethod (v,i,t) |> Some
    
            //| CompleteStarted (k0,_,t1), CompleteStarted (k1,_,t2) when k0=k1 && t1=t2 -> None
            | _,_ -> None
    
        let processChange (v : 'v) (change : VertexChanges<'d>) : RuntimeAction<'v> list =             
            Trace.Runtime.TraceEvent(Trace.Event.Verbose, 0, "Processing change of " + v.ToString() + ": " + (change.ToString()))
    
            match change with
            | Removed -> [ Remove v ]
    
            | New vs -> 
                vs |> MdMap.toSeq |> Seq.choose(fun (i, vis) ->
                    match vis.Status with
                    | CanStart time -> Delay(v,i,time) |> Some
                    | Started startTime -> Execute (v,i,startTime) |> Some
                    | _ -> None) |> Seq.toList
    
            | Modified (indices,oldvs,newvs,_) ->
                indices |> Set.toSeq |> Seq.choose(fun i -> 
                    let vis = newvs |> MdMap.tryFind i |> Option.get
                    let oldVis = oldvs |> MdMap.tryFind i  
                    processItemChange (v,i) oldVis vis) |> Seq.toList
    
            | ShapeChanged(oldvs, newvs, isConnectionChanged) ->
                let oldVis i = oldvs |> MdMap.tryFind i 
                newvs |> MdMap.toSeq |> Seq.choose(fun (i,vis) ->                    
                    processItemChange (v,i) (oldVis i) vis) |> Seq.toList
                
        update.Changes |> Map.fold(fun actions v vc -> (processChange v vc) @ actions) []

//////////////////////////////////////////////
// 
// Runtime
//
//////////////////////////////////////////////

open Angara
open Angara.Observable
open System.Collections.Generic
open System.Threading.Tasks.Schedulers
open Angara.States.Messages
open Angara.Trace

type internal Progress<'v>(v:'v, i:VertexIndex, progressReported : ObservableSource<'v*VertexIndex*float>) =
    interface IProgress<float> with
        member x.Report(p: float) = progressReported.Next(v,i,p)

// TODO: it is the Scheduler who should prepare the RuntimeContext for the target function.
[<AbstractClass>]
type Scheduler<'m when 'm:>ExecutableMethod and 'm:comparison> () =
    abstract Start : 'm * (unit -> unit) -> unit

    static member ThreadPool() : Scheduler<'m> = upcast ThreadPoolScheduler<'m>()
        
and [<Class>] ThreadPoolScheduler<'m when 'm:>ExecutableMethod and 'm:comparison>() =
    inherit Scheduler<'m>()

    static let scheduler = LimitedConcurrencyLevelTaskScheduler(System.Environment.ProcessorCount)
    static do Trace.Runtime.TraceInformation(sprintf "ThreadPoolScheduler limits concurrency level with %d" scheduler.MaximumConcurrencyLevel)
    static let mutable ExSeq = 0L   
    
    override x.Start (_, f: unit -> unit) =
        let id = System.Threading.Interlocked.Increment(&ExSeq)        
        System.Threading.Tasks.Task.Factory.StartNew((fun() ->
            try
                Trace.Runtime.TraceEvent(Trace.Event.Start, RuntimeId.SchedulerExecution, sprintf "Execution %d started" id)
                f()
                Trace.Runtime.TraceEvent(Trace.Event.Stop, RuntimeId.SchedulerExecution, sprintf "Execution %d finished" id)
            with ex ->
                Trace.Runtime.TraceEvent(Trace.Event.Error, RuntimeId.SchedulerExecution, sprintf "Execution %d failed: %O" id ex)
                raise ex), CancellationToken.None, Tasks.TaskCreationOptions.LongRunning, scheduler) |> ignore

[<Sealed>]
type Runtime<'m when 'm:>ExecutableMethod and 'm:comparison> (source:IObservable<State<'m,MethodOutput> * RuntimeAction<'m> list>, scheduler : Scheduler<'m>) =
    let messages = ObservableSource<Message<'m,MethodOutput>>()
    let cancels = Dictionary<'m*VertexIndex,CancellationTokenSource>()
    let progressReported = ObservableSource<'m*VertexIndex*float>()

    let progress (v:'m) (i:VertexIndex) : IProgress<float> = new Progress<'m>(v, i, progressReported) :> IProgress<float>
    
    let cancel (v:'m, i:VertexIndex) (cancels:Dictionary<'m*VertexIndex,CancellationTokenSource>) = 
        match cancels.ContainsKey (v,i) with
        | true -> 
            let cts = cancels.[v,i]
            Trace.Runtime.TraceEvent(Trace.Event.Verbose, 0, sprintf "Canceling delay or execution of %O.%A" v i)
            cts.Cancel()
            cancels.Remove(v,i) |> ignore
        | _ -> ()

    let cancelAll v (cancels:Dictionary<'m*VertexIndex,CancellationTokenSource>) = 
        let indices = cancels |> Seq.choose(fun k -> let u,i = k.Key in if u = v then Some(i) else None) |> Seq.toArray
        indices |> Seq.iter (fun i -> cancel (v,i) cancels)

    let delay (v,i) time =
        let delayMs = 0                    
        let run() = 
            Trace.Runtime.TraceEvent(Trace.Event.Verbose, 0, sprintf "Starting %O.%A at %d" v i time)
            messages.Next(Message.Start ({ Vertex = v; Index = i; CanStartTime = Some time }))
        if delayMs > 0 then
            let cts = new CancellationTokenSource()
            cancels.Add((v,i), cts)
            Async.Start(
                async {
                    do! Async.Sleep(delayMs)
                    if not (cts.IsCancellationRequested) then run() else ()
                }, cts.Token)
        else run()
       
    let postIterationSucceeded v index time result =
        Message.Iteration { Vertex = v; Index = index; StartTime = time; Result = result }
        |> messages.Next
        
    let postNoMoreIterations v index time = 
        Message.Succeeded { Vertex = v; Index = index; StartTime = time }
        |> messages.Next

    let postFailure v index time exn = 
        Message.Failed { Vertex = v; Index = index; StartTime = time; Failure = exn }
        |> messages.Next
    
    let convertArrays (inpTypes:Type list) (inputs:Input[]) =
        let restore (inp:Input, inpType:Type) : Artefact = 
            match inp with
            | Array array -> 
                let elType = inpType.GetElementType()
                let typedArr = Array.CreateInstance(elType, array.Length)
                for i in 0..array.Length-1 do
                    typedArr.SetValue(array.[i], i)
                upcast typedArr
            | Item item   -> item
            | NotAvailable -> failwith "Input artefacts for the method to execute are not available"
        Seq.zip inputs inpTypes |> Seq.map restore |> List.ofSeq

    let buildEvaluation (v:'m,index,time,state:State<'m,MethodOutput>) (cts:CancellationTokenSource) (doContinue:bool) = fun() ->
        Trace.Runtime.TraceEvent(Event.Start, RuntimeId.Evaluation, sprintf "Starting evaluation of %O.[%A]" v index)
                
        let cancellationToken = cts.Token
        RuntimeContext.replaceContext // todo: move to the scheduler
            { Token = cancellationToken
              ProgressReporter = progress v index } |> ignore

        let inputs = (v, index) |> Artefacts.getInputs (state.Vertices, state.Graph)
        try
            let checkpoint =
                if doContinue then
                    opt {
                        let! vis = state.Vertices |> DataFlowState.tryGet (v,index)
                        let! data = vis.Data
                        return! data.Checkpoint
                    } 
                else None
            let inArtefacts = inputs |> convertArrays (v:>IVertex).Inputs
            v.Execute(inArtefacts, checkpoint)
            |> Seq.takeWhile (fun _ -> not cancellationToken.IsCancellationRequested)
            |> Seq.iteri (fun i (output,chk) -> MethodOutput.Full(output, Some chk) |> postIterationSucceeded v index time)
                
            if not cancellationToken.IsCancellationRequested then 
                postNoMoreIterations v index time

            Trace.Runtime.TraceEvent(Event.Stop, RuntimeId.Evaluation, sprintf "SUCCEEDED execution of %O.[%A]" v index)
        with ex -> 
            Trace.Runtime.TraceEvent(Event.Error, RuntimeId.Evaluation, sprintf "FAILED execution of %O.[%A]" v index)
            ex |> postFailure v index time


    let performAction (state : State<'m,MethodOutput>) (action : RuntimeAction<'m>) = 
        match action with
        | Delay (v,slice,time) -> 
            cancel (v,slice) cancels
            delay (v,slice) time

        | StopMethod (v,slice,time) -> 
            cancel (v,slice) cancels

        | Execute (v,slice,time) -> 
            let cts = new CancellationTokenSource()
            let func = buildEvaluation (v,slice,time,state) cts false
            scheduler.Start (v, func)
            cancel (v,slice) cancels
            cancels.Add((v,slice), cts)

        | Continue (v,slice,time) -> 
            let cts = new CancellationTokenSource()
            let func = buildEvaluation (v,slice,time,state) cts true
            scheduler.Start (v, func)
            cancel (v,slice) cancels
            cancels.Add((v,slice), cts)

        | Remove v -> 
            cancelAll v cancels

    let perform (state : State<_,_>, actions : RuntimeAction<_> list) = 
        try
            actions |> Seq.iter (performAction state) 
        with exn ->
            Trace.Runtime.TraceEvent(Trace.Event.Critical, 0, sprintf "Execution runtime failed: %O" exn)
            messages.Error exn

    let mutable subscription = null

    do
        subscription <- source |> Observable.subscribe perform 


    interface IDisposable with
        member x.Dispose() = 
            subscription.Dispose()
            cancels.Values |> Seq.iter(fun t -> t.Cancel(); (t :> IDisposable).Dispose())
            messages.Completed()
            progressReported.Completed()

    member x.Evaluation = messages.AsObservable
    member x.Progress = progressReported.AsObservable


module internal Helpers = 
    open Angara.States.Messages

    let internal asAsync (create: ReplyChannel<_> -> Message<_,_>) (source:ObservableSource<_>) : Async<Response<_>> = 
        Async.FromContinuations(fun (ok, _, _) -> create ok |> source.Next)

    let internal unwrap (r:Async<Response<_>>) : Async<_> = 
        async{
            let! resp = r
            return match resp with Success s -> s | Exception exn -> raise exn
        }

open Helpers

[<Class>]
type Engine<'m when 'm:>ExecutableMethod and 'm:comparison>(initialState : State<'m, MethodOutput>, scheduler:Scheduler<'m>) =
    
    let messages = ObservableSource()

    let matchOutput (a:MethodOutput) (b:MethodOutput) (outRef:OutputRef) = 
        match a.TryGet outRef, b.TryGet outRef with
        | Some artA, Some artB -> LanguagePrimitives.GenericEqualityComparer.Equals(a,b)
        | _ -> false
            
    let stateMachine = Angara.States.StateMachine.CreateSuspended messages.AsObservable initialState
    let runtimeActions = stateMachine.Changes |> Observable.map (fun update -> update.State, Analysis.analyzeChanges update)
    let runtime = new Runtime<'m>(runtimeActions, scheduler)

    let mutable sbs = null

    do
        sbs <- runtime.Evaluation.Subscribe(messages.Next)

    member x.State = stateMachine.State
    member x.Changes = stateMachine.Changes
    member x.Progress = runtime.Progress

    member x.Start() = stateMachine.Start()

    member x.Post(m:Messages.Message<'m,MethodOutput>) =
        messages.Next m

    interface IDisposable with
        member x.Dispose() = 
            sbs.Dispose()
            (stateMachine :> IDisposable).Dispose()
            (runtime :> IDisposable).Dispose()


module Control =
    
    open System.Reactive.Linq
    open Angara.Data

    type FlowFailedException(errors : exn seq) =        
        inherit System.AggregateException("Some methods of the flow failed", errors |> Seq.toArray)

    let internal vectorToArtefact (vector:MdMap<int, Artefact>, rank:int, artType: System.Type) : Artefact = 
        if rank = 0 then vector.AsScalar()
        else 
            let objArr = vector |> MdMap.toJaggedArray
            let arrTypes = Seq.unfold(fun (r : int, t : System.Type) ->
                if r > 0 then Some(t, (r-1, t.MakeArrayType())) else None) (rank, artType) |> Seq.toList

            let rec cast (eltType:System.Type) (r:int) (arr:obj[]) : System.Array =
                let n = arr.Length
                if r = 1 then
                    let tarr = System.Array.CreateInstance(eltType, n)
                    if n > 0 then arr.CopyTo(tarr, 0)
                    tarr
                else             
                    let tarr = System.Array.CreateInstance(arrTypes.[r-1], n)
                    for i = 0 to n-1 do tarr.SetValue(cast eltType (r-1) (arr.[i]:?>obj[]), i)
                    tarr
            upcast cast artType rank objArr

    let internal extractOutput<'m when 'm:>ExecutableMethod and 'm:comparison> (vertex:'m) (outRef: OutputRef) (state:State<'m,MethodOutput>) = 
        let rank = Angara.Graph.vertexRank vertex state.Graph.Structure
        let artType = (vertex :> IVertex).Outputs.[outRef]
        let vs = state.Vertices.[vertex]                    
        let md = vs |> MdMap.map (fun vis -> vis.Data.Value.Artefacts.TryGet(outRef).Value)
        vectorToArtefact (md, rank, artType)


    let pickFinal<'m when 'm:>ExecutableMethod and 'm:comparison> states =
        let isFinal (update : StateUpdate<'m,MethodOutput>) =
            let failedMethods = 
                update.State.Vertices 
                |> Map.toSeq 
                |> Seq.map(fun (_,vs) -> 
                    vs 
                    |> Angara.Data.MdMap.toSeq
                    |> Seq.choose(fun (_, vis) ->
                        match vis.Status with
                        | VertexStatus.Incomplete (IncompleteReason.ExecutionFailed e) -> Some e
                        | _ -> None))                  
                |> Seq.concat
                |> Seq.toList
            match failedMethods with
            | [] ->
                update.State.Vertices 
                |> Map.toSeq
                |> Seq.forall(fun (_,vs) -> 
                    vs 
                    |> Angara.Data.MdMap.toSeq
                    |> Seq.forall(fun (_, vis) ->
                        match vis.Status with
                        | VertexStatus.Final _ -> true
                        | _ -> false))
            | errors -> raise (new FlowFailedException(errors))

        (Observable.FirstAsync(states, new Func<StateUpdate<'m,MethodOutput>, bool>(isFinal)) |> Observable.map(fun update -> update.State)).GetAwaiter()
    

    let outputScalar<'m,'a when 'm:>ExecutableMethod and 'm:comparison> (m:'m, outRef:OutputRef) (state:State<'m,MethodOutput>) : 'a =
        let output = extractOutput m outRef state
        unbox output

    let runToFinalIn<'m when 'm:>ExecutableMethod and 'm:comparison> (scheduler:Scheduler<'m>)  (state:State<'m, MethodOutput>) =
        use engine = new Engine<'m>(state, scheduler)
        let final = pickFinal engine.Changes   
        engine.Start()
        final.GetResult()

    let runToFinal<'m when 'm:>ExecutableMethod and 'm:comparison> (state:State<'m, MethodOutput>) =
        runToFinalIn (ThreadPoolScheduler.ThreadPool()) state