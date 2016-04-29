module Angara.Execution

open System
open System.Threading
open Angara.Graph
open Angara.StateMachine
open Angara.Trace
open Angara.RuntimeContext


type Artefact = obj

type Work<'v,'d when 'v:comparison and 'v:>IVertex> =
    { Graph : DataFlowGraph<'v>
      State : DataFlowState<'v,'d>
    }

type Output = 
    | Full    of Artefact list
    | Partial of (Artefact option) list
    member x.TryGet (outRef:OutputRef) =
        match x with
        | Full artefacts -> Some(artefacts.[outRef])
        | Partial artefacts -> if outRef < artefacts.Length then artefacts.[outRef] else None

type Input = 
    | NotAvailable
    | Item of Artefact
    | Array of Artefact array  

type ResumableState = obj

type CompoundMethod<'v,'d when 'v:comparison and 'v:>IVertex> = 
    { Graph: DataFlowGraph<'v>
      State: DataFlowState<'v,VertexState<'d>>
      Inputs: ('v * InputRef) list
      Outputs: ('v * OutputRef) list
    }

   
[<Interface>]
type IExecutableVertex<'v, 'd when 'v:comparison and 'v:>IExecutableVertex<'v,'d>> =
    inherit IVertex
    abstract Method : Input * 't option -> Output * 't option // Method<'v,'d> 
       

[<Class>] 
type ExecutableVertexData<'v when 'v:comparison and 'v:>IExecutableVertex<'v,ExecutableVertexData<'v>>> private (output: Output, resumableState: ResumableState option, compoundState: State<'v,ExecutableVertexData<'v>> option) =
    interface IVertexData
    member x.Output: Output = output
    member x.Data : 't option
    
    static member FromOutput(output: Output) =
        ExecutableVertexData<'v>(output, None, None)

    static member Resumable(output: Output, resumableState: ResumableState option) =
        ExecutableVertexData<'v>(output, resumableState, None)

    static member Compound(output: Output, compoundState: State<'v,ExecutableVertexData<'v>>) =
        ExecutableVertexData<'v>(output, None, Some compoundState)

    static member Empty = ExecutableVertexData<'v>(Partial [], None, None)
 

module Artefacts =
    open Angara.Option
    open Angara.Data
    
    let internal tryGetOutput<'v when 'v:comparison and 'v:>IExecutableVertex<'v,ExecutableVertexData<'v>>> (edge:Edge<_>) (i:VertexIndex) (state:DataFlowState<'v,VertexState<ExecutableVertexData<'v>>>) : Artefact option =
        opt {
            let! vs = state |> Map.tryFind edge.Source
            let! vis = vs |> MdMap.tryFind i
            let! data = vis.Data
            return! data.Output.TryGet edge.OutputRef
        }

    /// Sames as getOutput, but "i" has rank one less than rank of the source vertex,
    /// therefore result is an array of artefacts for all available indices complementing "i".
    let internal tryGetReducedOutput<'v when 'v:comparison and 'v:>IExecutableVertex<'v,ExecutableVertexData<'v>>> (edge:Edge<_>) (i:VertexIndex) (state:DataFlowState<'v,VertexState<ExecutableVertexData<'v>>>) : Artefact[] option =
        match state |> Map.tryFind edge.Source with
        | Some svs ->
            let r = i.Length
            let items = 
                svs 
                |> MdMap.startingWith i 
                |> MdMap.toSeq 
                |> Seq.filter(fun (j,_) -> j.Length = r + 1) 
                |> Seq.map(fun (j,vis) -> (List.last j, vis.Data |> Option.bind(fun data -> data.Output.TryGet edge.OutputRef))) 
                |> Seq.toList
            let n = items |> Seq.map fst |> Seq.max
            let map = items |> Map.ofList
            let artefacts = Seq.init (n+1) (fun i -> i) |> Seq.map(fun i -> match map.TryFind(i) with | Some(a) -> a | None -> None) |> Seq.toList
            if artefacts |> List.forall Option.isSome then artefacts |> Seq.map Option.get |> Seq.toArray |> Option.Some
            else None
        | None -> None

    /// Returns the vertex' output artefact as n-dimensional typed jagged array, where n is a rank of the vertex.
    /// If n is zero, returns the typed object itself.
    let getMdOutput<'v when 'v:comparison and 'v:>IExecutableVertex<'v,ExecutableVertexData<'v>>> (v:'v) (outRef: OutputRef) (graph:DataFlowGraph<'v>, state:DataFlowState<'v,VertexState<ExecutableVertexData<'v>>>) = 
        let vector = state |> Map.find v |> MdMap.map (fun vis -> vis.Data.Value.Output.TryGet(outRef).Value)
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
    let getInputs (state: DataFlowState<'v,VertexState<_>>, graph: DataFlowGraph<'v>) (v:'v, i:VertexIndex) : Input[] =
        let inputs = Array.init v.Inputs.Length (fun i -> if v.Inputs.[i].IsArray then Input.Array (Array.empty) else Input.NotAvailable)
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

type internal RuntimeAction<'v> =
    /// Send "Start" message after a delay
    | Delay     of 'v * VertexIndex * TimeIndex
    /// Execute a method and send "Succeeded" or "Failed" 
    | Execute   of 'v * VertexIndex * TimeIndex 
    /// Stop the execution.
    | StopMethod of 'v * VertexIndex * TimeIndex
    /// Forget about the method
    | Remove    of 'v

/// Analyzes state machine changes and provides a list of action to be performed by an execution runtime.
/// All methods are executed when have status "CanStart".
let internal analyzeChanges (state: State<'v,'d>, changes: Changes<'v,'d>) : RuntimeAction<'v> list =
    failwith ""
//            let processItemChange (v: 'v, i:VertexIndex) (oldvis: (VertexItemState) option) (vis: VertexItemState) : RuntimeAction option =
//                let oldStatus = 
//                    match oldvis with
//                    | Some oldvis -> oldvis.Status
//                    | None -> Incomplete IncompleteReason.UnassignedInputs
//
//                match oldStatus, vis.Status with
//                | CanStart t1, CanStart t2 when t1 <> t2 -> Delay (v,i,t2) |> Some
//
//                | _, CanStart t ->                          Delay (v,i,t) |> Some
//            
//                | CanStart _, Started t ->                  Execute (v,i,t,None) |> Some
//
//                | CompleteStarted (k0,_,t1), CompleteStarted (k1,_,t2) when k0=k1 && t1=t2 -> None
//            
//                | _, CompleteStarted (k,_,t) -> // executes the transient method and sends "Succeeded"/"Failed" when it finishes
//                    if k.IsSome then failwith "Transient iterative methods are not supported" 
//                    Execute (v,i,t,None) |> Some
//
//                | Complete (Some(_),_), Continues (k,_,t) -> // resumes an iterative method
//                    let initial = match k with 0 -> None | _ -> Some(k, vis |> output |> Array.ofList)
//                    Execute (v,i,t,initial) |> Some
//
//                | Continues (_,_,t), Complete _     // stops iterations
//                | Started t, Incomplete (IncompleteReason.Stopped) -> StopMethod (v,i,t) |> Some
//
//                | _,_ -> None
//
//            let processChange (v : 'v) (change : VertexChanges) : RuntimeAction list =             
//                Trace.Runtime.TraceEvent(Trace.Event.Verbose, 0, "Processing change of " + v.ToString() + ": " + (change.ToString()))
//
//                match change with
//                | Removed -> [ Remove v ]
//
//                | New vs -> 
//                    vs.ToSeq() |> Seq.choose(fun (i, vis) ->
//                        match vis.Status with
//                        | CanStart time -> Delay(v,i,time) |> Some
//                        | Started startTime -> Execute (v,i,startTime,None) |> Some
//                        | _ -> None) |> Seq.toList
//
//                | Modified (indices,oldvs,newvs,_) ->
//                    indices |> Set.toSeq |> Seq.choose(fun i -> 
//                        let vis = newvs.TryGetItem i |> Option.get
//                        let oldVis = oldvs.TryGetItem i 
//                        processItemChange (v,i) oldVis vis) |> Seq.toList
//
//                | ShapeChanged(oldvs, newvs, isConnectionChanged) ->
//                    let oldVis i = oldvs.TryGetItem i 
//                    newvs.ToSeq() |> Seq.choose(fun (i,vis) ->                    
//                        processItemChange (v,i) (oldVis i) vis) |> Seq.toList
//            
//            changes |> Map.fold(fun actions v vc -> (processChange v vc) @ actions) []

//////////////////////////////////////////////
// 
// Runtime
//
//////////////////////////////////////////////

type EvaluationCallback<'d> = 
    { OnSucceeded       : 'd -> unit
      OnIteration       : 'd -> unit
      OnNoMoreIterations: unit -> unit
      OnFailure         : System.Exception -> unit 
      OnProgress        : System.IProgress<float> }


type EvaluationTarget<'v,'d when 'v:comparison and 'v:>IVertex> = 
    { Vertex : 'v
      Index : VertexIndex
      Snapshot : State<'v,'d> }
    
open Angara.Option
open Angara.Observable
open System.Collections.Generic

[<Interface>]
type IRuntime<'v,'d when 'v:comparison and 'v:>IVertex> =        
    inherit IDisposable
    abstract Evaluation : IObservable<Message<'v,'d>>
    abstract Progress : IObservable<'v*VertexIndex*float>

[<Interface>]
type IEngine<'v,'d when 'v:comparison and 'v:>IVertex and 'd:>IVertexData> =
    inherit IDisposable
    abstract StateMachine : IStateMachine<'v,'d>
    abstract Runtime : IRuntime<'v,'d>

    abstract Start : unit -> unit

[<Interface>]
type IEngineFactory<'v,'d when 'v:comparison and 'v:>IVertex and 'd:>IVertexData> =
    abstract CreateSuspended : initialState:(DataFlowGraph<'v> * DataFlowState<'v,VertexState<'d>>) -> IEngine<'v,'d>



///////////////////////////////////////////
//
// Compound Method
//
///////////////////////////////////////////

open Angara.Data

[<Interface>]
type ICompoundMethodEvaluator<'v,'d when 'v:comparison and 'v:>IVertex> =
    abstract EvaluateAsync : Artefact list -> CompoundMethod<'v,'d> -> EvaluationCallback<'d> -> Async<unit>


type CompoundMethodEvaluator<'v when 'v:comparison and 'v:>IExecutableVertex<'v,ExecutableVertexData<'v>>>(createMakeValue : Artefact -> Type -> 'v, engineFactory : IEngineFactory<'v,ExecutableVertexData<'v>>) =
    
    interface ICompoundMethodEvaluator<'v,ExecutableVertexData<'v>> with

        member x.EvaluateAsync inputs c callback = 
            if inputs.Length <> c.Inputs.Length then failwith "Number of input artefacts in the flow state differs from number of the compound method inputs"

            let prepareInputs ((graph:DataFlowGraph<'v>, state:DataFlowState<'v,VertexState<ExecutableVertexData<'v>>>), connects:AlterConnection<'v> list) (((v,inRef),a) : ('v*InputRef)*Artefact) =
                let mkv : 'v = createMakeValue a v.Inputs.[inRef]
                let graph = graph.Add mkv
                let connects = AlterConnection.ItemToItem((v, inRef), (mkv, 0)) :: connects
                let state = state.Add(mkv, VertexState.Complete (ExecutableVertexData.FromOutput(Output.Full [a])) |> MdMap.scalar)
                (graph,state),connects

            let merge,connect = Seq.zip c.Inputs inputs |> Seq.fold prepareInputs ((DataFlowGraph<'v>.Empty,Map.empty), [])

            async{
                use engine = engineFactory.CreateSuspended (c.Graph, c.State)
                engine.Start()

                do! engine.StateMachine.AlterAsync ([], [], merge, connect) 
                let! finalWork = awaitFinal engine.StateMachine.Changes

                // todo: if finalWork is complete:
                match finalWork.Data.GetStatus() with
                | 
                let outputs = c.Outputs |> List.map (fun (v,outRef) -> Artefacts.getMdOutput v outRef (finalWork.Graph, finalWork.Data))
                callback.OnSucceeded (ExecutableVertexData.Compound(Full outputs, finalWork))

//            return 
//                match stableWrk with
//                | Engine.StableWork.Complete snapshot -> 
//                    outputs |> List.map (fun (v,outRef) -> extractOutput v outRef snapshot) |> Array.ofList, (snd snapshot) :> Angara.Execution.CustomState
//                | Engine.StableWork.Incomplete (_,reasons) -> 
//                    let exns = reasons |> Seq.map (fun (v,i,r) -> 
//                                            match r with
//                                            | ExecutionFailed exn -> 
//                                                Trace.Runtime.TraceEvent(Angara.Trace.Event.Error, 100, "Compound method execution failed: " + (exn.ToString()))
//                                                Some(new Angara.Execution.MethodException(v,i, exn) :> System.Exception)
//                                            | UnassignedInputs -> Some(new Angara.Execution.MethodException(v, i, new System.Exception("Method has unassigned inputs")) :> System.Exception)
//                                            | OutdatedInputs -> Some(new Angara.Execution.MethodException(v, i, new System.Exception("Method has outdated inputs")) :> System.Exception)
//                                            | Stopped -> 
//                                                Trace.Runtime.TraceEvent(Angara.Trace.Event.Error, 100, "Compound method execution stopped")
//                                                Some(new Angara.Execution.MethodException(v,i, new System.OperationCanceledException()) :> System.Exception)
//                                            | TransientInputs -> None)
//                                        |> Seq.filter (Option.isSome) |> Seq.map (Option.get) |> Seq.toArray
//                    raise (new Angara.Execution.CustomBehaviorException("Compound method could not be complete", exns, snd engine.Current))
            } 



///////////////////////////////////////////
//
// Implementation
//
///////////////////////////////////////////

type internal IScheduler =
    abstract Start : (unit -> unit) -> unit
    abstract Start : Async<unit> -> unit

type internal Progress<'v>(v:'v, i:VertexIndex, progressReported : ObservableSource<'v*VertexIndex*float>) =
    interface IProgress<float> with
        member this.Report(p: float) = progressReported.Next(v,i,p)

type internal RuntimeImpl<'v when 'v:comparison and 'v:>IExecutableVertex<'v,ExecutableVertexData<'v>>>
    (source:IObservable<State<'v,ExecutableVertexData<'v>> * RuntimeAction<'v> list>, scheduler : IScheduler, engineFactory : IEngineFactory<'v,ExecutableVertexData<'v>>) =

    let messages = ObservableSource<Message<'v,ExecutableVertexData<'v>>>()
    let cancels = Dictionary<'v*VertexIndex,CancellationTokenSource>()
    let progressReported = ObservableSource<'v*VertexIndex*float>()

    let progress (v:'v) (i:VertexIndex) : IProgress<float> = new Progress<'v>(v, i, progressReported) :> IProgress<float>
    
    let cancel (v:'v, i:VertexIndex) (cancels:Dictionary<'v*VertexIndex,CancellationTokenSource>) = 
        match cancels.ContainsKey (v,i) with
        | true -> 
            let cts = cancels.[v,i]
            Trace.Runtime.TraceEvent(Trace.Event.Verbose, 0, sprintf "Canceling delay or execution of %O.%A" v i)
            cts.Cancel()
            cancels.Remove(v,i) |> ignore
        | _ -> ()

    let cancelAll v (cancels:Dictionary<'v*VertexIndex,CancellationTokenSource>) = 
        let indices = cancels |> Seq.choose(fun k -> let u,i = k.Key in if u = v then Some(i) else None) |> Seq.toArray
        indices |> Seq.iter (fun i -> cancel (v,i) cancels)

    let delay (v,i) time =
        let delayMs = 0                    
        let trace() = Trace.Runtime.TraceEvent(Trace.Event.Verbose, 0, sprintf "Starting %O.%A at %d" v i time)
        if delayMs > 0 then
            let cts = new CancellationTokenSource()
            cancels.Add((v,i), cts)
            Async.Start(
                async {
                    do! Async.Sleep(delayMs)
                    if not (cts.IsCancellationRequested) then
                        trace()
                        messages.Next(Message.Start(v, Some(i,Some time), ignore))
                    else ()
                }, cts.Token)
        else trace(); messages.Next(Message.Start(v, Some(i,Some time), ignore))        
       
    let onSuccess v index time snapshot = messages.Next(Message.Succeeded(v, index, true, snapshot, time))
    let onSuccessP v index time snapshot = messages.Next(Message.Succeeded(v, index, true, snapshot, time))
    let onIteration v index time snapshot = messages.Next(Message.Succeeded(v, index, false, snapshot, time))
    let noMoreIterations v index time () = messages.Next(Message.Succeeded(v, index, true, ExecutableVertexData.Empty, time))
    let onFailure v index time ex = messages.Next(Message.Failed(v, index, ex, time))
    let reaction v index time : EvaluationCallback<ExecutableVertexData<'v>> = 
        { OnSucceeded = onSuccess v index time
          OnIteration = onIteration v index time
          OnNoMoreIterations = noMoreIterations v index time
          OnFailure = onFailure v index time
          OnProgress = progress v index }

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

    let buildEvaluation (callback:EvaluationCallback<ExecutableVertexData<'v>>) (target:EvaluationTarget<'v,ExecutableVertexData<'v>>) (cts:CancellationTokenSource) = 
        let cancellationToken = cts.Token
        let action() = 
            Trace.Runtime.TraceEvent(Event.Start, RuntimeId.Evaluation, sprintf "Starting evaluation of %O.[%A]" target.Vertex target.Index)
                
            RuntimeContext.replaceContext 
                { Token = cancellationToken
                  ProgressReporter = callback.OnProgress
                  Logger = null } |> ignore

            let inputs = (target.Vertex, target.Index) |> Artefacts.getInputs (target.Snapshot.Status, target.Snapshot.Graph)
            try
                match target.Vertex.Method with
                | Function f -> 
                    let output = f(inputs |> convertArrays target.Vertex.Inputs)
                    callback.OnSucceeded(ExecutableVertexData.FromOutput (Full output)) 
                | Iterative f -> 
                    let n = 
                        opt{
                            let! vis = target.Snapshot.Status |> DataFlowState.tryGet (target.Vertex, target.Index)
                            return! vis.Status.TryGetIterationCount
                        }

                    f(inputs |> convertArrays target.Vertex.Inputs)                                                               
                    |> Seq.skip (defaultArg n 0)
                    |> Seq.takeWhile (fun _ -> not cancellationToken.IsCancellationRequested)
                    |> Seq.iter (fun output -> callback.OnIteration(ExecutableVertexData.FromOutput (Full output)))
                    if not(cancellationToken.IsCancellationRequested) then callback.OnNoMoreIterations()

                | Resumable f -> 
                    let resumableState = 
                        opt{
                            let! vis = target.Snapshot.Status |> DataFlowState.tryGet (target.Vertex, target.Index)
                            let! k = vis.Status.TryGetIterationCount
                            let! data = vis.Data
                            match k, data.Output with
                            | k, Full artefacts when k > 0 -> return artefacts, data.ResumableState
                            | _ -> return! None
                        }

                    f(resumableState, inputs |> convertArrays target.Vertex.Inputs) 
                    |> Seq.takeWhile (fun _ -> not cancellationToken.IsCancellationRequested) 
                    |> Seq.iter (fun (artefacts, resumable) -> callback.OnIteration(ExecutableVertexData.Resumable (Full artefacts, resumable)))
                    if not(cancellationToken.IsCancellationRequested) then callback.OnNoMoreIterations() 
                
                | Compound с ->
                    buildCompoundEvaluation()


                Trace.Runtime.TraceEvent(Event.Stop, RuntimeId.Evaluation, sprintf "Successfully completed execution of %O.[%A]" target.Vertex target.Index)
            with ex -> 
                Trace.Runtime.TraceEvent(Event.Stop, RuntimeId.Evaluation, sprintf "Completed with failure execution of %O.[%A]" target.Vertex target.Index)
                callback.OnFailure(ex)
           
        action

    let performAction (state : State<'v,ExecutableVertexData<'v>>) (action : RuntimeAction<'v>) = 
        match action with
        | Delay (v,slice,time) -> 
            cancel (v,slice) cancels
            delay (v,slice) time

        | StopMethod (v,slice,time) -> 
            cancel (v,slice) cancels

        | Execute (v,slice,time) -> 
            let target : EvaluationTarget<'v,ExecutableVertexData<'v>> = 
                { Vertex = v
                  Index = slice
                  Snapshot = state}
            let cts = new CancellationTokenSource()
            let func = buildEvaluation (reaction v slice time) target cts 
            scheduler.Start func
            cancel (v,slice) cancels
            cancels.Add((v,slice), cts)

        | Remove v -> 
            cancelAll v cancels

    let perform (state : State<'v,ExecutableVertexData<'v>>, actions : RuntimeAction<'v> list) = 
        try
            actions |> Seq.iter (performAction state) 
        with exn ->
            Trace.Runtime.TraceEvent(Trace.Event.Critical, 0, sprintf "Execution runtime failed: %O" exn)
            messages.Error exn
//            match subscription with
//            | null -> ()
//            | _ -> subscription.Dispose()


    interface IRuntime<'v,ExecutableVertexData<'v>> with
        member x.Dispose() = 
            //subscription.Dispose()
            cancels.Values |> Seq.iter(fun t -> t.Cancel(); (t :> IDisposable).Dispose())
            messages.Completed()
            progressReported.Completed()

        member x.Evaluation = messages.AsObservable
        member x.Progress = progressReported.AsObservable
        
    

//let Run<'v,'d when 'v:comparison and 'v:>IVertex> (source:IObservable<State<'v,'d> * Changes<'v,'d>>) : IRuntime<'v,'d> =
//    let actions = source |> Observable.map (fun (s,c) -> s, analyzeChanges (s,c))
//    let evaluator = ()
//    upcast RuntimeImpl(actions, evaluator)
//    
