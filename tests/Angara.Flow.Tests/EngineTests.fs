module ``Engine tests``

open NUnit.Framework

open Angara.Graph
open Angara.States
open Angara.Execution
open System.Diagnostics
open System.Threading


type Dag = DirectedAcyclicGraph<Method, Edge<Method>>

type MethodMakeValue<'a>(value: 'a) =
    inherit Method(System.Guid.NewGuid(), [], [typeof<'a>])

    override x.Execute(_, _) =
        upcast [ [box value], null ]



[<Test; Category("CI")>]
let ``Engine executes a flow containing a single method`` () =
    let methodOne : Method = upcast MethodMakeValue System.Math.PI
    let flowGraph = DataFlowGraph(Dag([methodOne, []] |> Map.ofList))

    // The initial state has no value for the method. 
    // It must be built automatically by the engine when it is started.
    // Automatic initial state of the method will be CanStart.
    let flowState = DataFlowState([])

    let state = 
        { TimeIndex = 0UL
          Graph = flowGraph
          FlowState = flowState          
        }
    use engine = new Engine(state, Scheduler.ThreadPool())
    
    let ev = new AutoResetEvent(false)
    engine.Changes.Add(fun update -> 
        match update.State.FlowState |> Map.forall(fun _ vs -> vs.AsScalar().Status.IsUpToDate) with
        | true -> ev.Set() |> ignore
        | false -> ())
    
    engine.Start()

    ev.WaitOne() |> ignore


[<Test; Category("CI")>]
let ``Engine normalizes given initial state when the engine is started`` () =
    Assert.Inconclusive("Test is incomplete")
