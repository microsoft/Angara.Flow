module ``Engine tests``

open NUnit.Framework

open Angara.Graph
open Angara.States
open Angara.Execution


type Dag = DirectedAcyclicGraph<Method, Edge<Method>>

type MethodMakeValue<'a>(value: 'a) =
    inherit Method(System.Guid.NewGuid(), [], [typeof<'a>])

    override x.Execute(_, _) =
        upcast [ [box value], null ]



[<Test; Category("CI")>]
let ``Engine normalizes given initial state when the engine is started`` () =
    let methodOne : Method = upcast MethodMakeValue System.Math.PI
    let flowGraph = DataFlowGraph(Dag([methodOne, []] |> Map.ofList))

    // The initial state has no value for the method. 
    // It must be built automatically by the engine when it is started.
    // Automatic initial state of the method will be CanStart.
    let flowState = DataFlowState([methodOne, MdVertexState.Empty])

    let state = 
        { TimeIndex = 0UL
          Graph = flowGraph
          FlowState = flowState          
        }
    use engine = new Engine(state, Scheduler.ThreadPool())
    engine.Changes.Add(printfn "%A")
    engine.Start()
    

    Assert.Inconclusive("Test is incomplete")



