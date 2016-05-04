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

    

//let Run<'v,'d when 'v:comparison and 'v:>IVertex> (source:IObservable<State<'v,'d> * Changes<'v,'d>>) : IRuntime<'v,'d> =
//    let actions = source |> Observable.map (fun (s,c) -> s, analyzeChanges (s,c))
//    let evaluator = ()
//    upcast RuntimeImpl(actions, evaluator)
//    
