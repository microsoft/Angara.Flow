module ``Engine tests``

open NUnit.Framework

open Angara.Graph
open Angara.States
open Angara.Execution
open System.Diagnostics
open System.Threading
open System
open System.Reactive.Linq


type Dag = DirectedAcyclicGraph<Method, Edge<Method>>

type MethodMakeValue<'a>(value: 'a) =
    inherit Method(System.Guid.NewGuid(), [], [typeof<'a>])

    override x.Execute(_, _) =
        seq{ yield [box value], null }

type MethodOp<'a,'b>(f: 'a->'b) =
    inherit Method(System.Guid.NewGuid(), [typeof<'a>], [typeof<'b>])

    override x.Execute(args, _) =
        match args with
        | [a] -> 
            let value : 'a = unbox a
            seq{ yield [box (f value)], null }
        | _ -> failwith "Incorrect number of arguments"

type MethodMakeValueIter<'a>(values: 'a seq) =
    inherit Method(System.Guid.NewGuid(), [], [typeof<'a>])

    override x.Execute(_, _) =
        values |> Seq.map(fun v -> [unbox v], unbox v)


let pickFinal states =
    let isFinal (update : StateUpdate<Method,MethodOutput>) =
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
        | errors -> failwithf "Some methods failed: %A" errors

    Observable.FirstAsync(states, new Func<StateUpdate<Method,MethodOutput>, bool>(isFinal))
    |> Observable.map(fun update -> update.State)

let output (m:Method, output:OutputRef) (state:State<Method,MethodOutput>) =
    unbox (state.Vertices.[m].AsScalar().Data.Value.Output.TryGet(output).Value)

let runToCompletion state =
    use engine = new Engine(state, Scheduler.ThreadPool())
    let final = pickFinal engine.Changes   
    engine.Start()
    final.GetAwaiter().GetResult()



//--------------------------------------------------------------------------------------------------------------------
// Tests


[<Test; Category("CI")>]
let ``Engine executes a flow containing a single method`` () =
    let methodOne : Method = upcast MethodMakeValue System.Math.PI
    let graph = 
        FlowGraph.empty()
        |> FlowGraph.add methodOne

    // The initial state has no value for the method. 
    // It must be built automatically by the engine when it is started.
    // Automatic initial state of the method will be CanStart.
    let vertices = VerticesState([])

    let state = 
        { TimeIndex = 0UL
          Graph = graph
          Vertices = vertices          
        }
    
    let s = runToCompletion state
    let result : float = s |> output (methodOne,0)
    Assert.AreEqual(System.Math.PI, result, "Execution result")


[<Test; Category("CI")>]
let ``Engine executes a flow of two chained methods`` () =
    let methodOne : Method = upcast MethodMakeValue<float> System.Math.PI
    let methodInc : Method = upcast MethodOp<float,float> (fun a -> a + 1.0)

    let graph = 
        FlowGraph.empty()
        |> FlowGraph.add methodOne
        |> FlowGraph.add methodInc
        |> FlowGraph.connect (methodOne, 0) (methodInc, 0)

    let state = 
        { TimeIndex = 0UL
          Graph = graph
          Vertices = VerticesState []           
        }
        
    let s = runToCompletion state
    let result : float = s |> output (methodInc,0)
    Assert.AreEqual(System.Math.PI + 1.0, result, "Execution result")



[<Test; Category("CI")>]
let ``Engine executes an iterative method and we can see intermediate outputs`` () =
    let pi = System.Math.PI
    let iters = [ for i in 0..3 -> float(i)/3.0 * pi ]
    let methodOne : Method = upcast MethodMakeValueIter<float> iters

    let graph = 
        FlowGraph.empty()
        |> FlowGraph.add methodOne
    let state = 
        { TimeIndex = 0UL
          Graph = graph
          Vertices = VerticesState []           
        }
        
    use engine = new Engine(state, Scheduler.ThreadPool())
    let iterations = 
        engine.Changes.TakeWhile(fun update ->
            match update.State.Vertices.[methodOne].AsScalar().Status with 
            | VertexStatus.Final _ -> false
            | _ -> true)
        |> Observable.choose(fun update ->
            match update.Changes.[methodOne] with
            | VertexChanges.Modified (_,_,vs,_) when vs.AsScalar().Data.IsSome ->
                Some (vs.AsScalar().Data.Value.Output.TryGet(0).Value)
            | _ -> None)
        |> Observable.ToList
        
    engine.Start()

    let actualIters = iterations.GetAwaiter().GetResult() |> Seq.toList
    Assert.AreEqual(iters, actualIters, "Iterations")    


[<Test; Category("CI")>]
let ``Engine executes a flow with vector methods`` () =
    let pi = System.Math.PI    
    let input = [| for k in 0..3 -> float(k)*pi |]

    let methodMakeArr : Method = upcast MethodMakeValue<float[]> input
    let methodInc : Method = upcast MethodOp<float,float> (fun a -> a + 1.0)
    let methodSum : Method = upcast MethodOp<float[],float> Array.sum

    let graph = 
        FlowGraph.empty()
        |> FlowGraph.add methodMakeArr
        |> FlowGraph.add methodInc
        |> FlowGraph.add methodSum
        // An output of type `float[]` is connected to an input of type `float`,
        // so the latter is vectorized to be called for each of the input element.
        |> FlowGraph.connect (methodMakeArr, 0) (methodInc, 0)
        // A vector of float values is connected to an input of type `float[]`, 
        // so `methodSum` will reduce the vector.
        |> FlowGraph.connect (methodInc, 0) (methodSum, 0)

    let state = 
        { TimeIndex = 0UL
          Graph = graph
          Vertices = VerticesState []           
        }
        
    let s = runToCompletion state
    let result : float = s |> output (methodSum,0)
    Assert.AreEqual(input |> Array.map (fun v -> v+1.0) |> Array.sum, result, "Execution result")


[<Test; Category("CI")>]
let ``Engine executes a flow with vector of length zero`` () =
    // The source method will produce an empty array.
    // Then this array is passed to a vectorized method that increments each element of the input array.
    // In case of empty input array, the vectorized method produces an empty array as well.
    // Thus the final sink method is called for an empty array and produces a default value, 0.
    let input = Array.empty

    let methodMakeArr : Method = upcast MethodMakeValue<float[]> input
    let methodInc : Method = upcast MethodOp<float,float> (fun a -> a + 1.0)
    let methodSum : Method = upcast MethodOp<float[],float> (fun arr -> if arr.Length = 0 then 0.0 else Array.sum arr)

    let graph = 
        FlowGraph.empty()
        |> FlowGraph.add methodMakeArr
        |> FlowGraph.add methodInc
        |> FlowGraph.add methodSum
        |> FlowGraph.connect (methodMakeArr, 0) (methodInc, 0)
        |> FlowGraph.connect (methodInc, 0) (methodSum, 0)

    let state = 
        { TimeIndex = 0UL
          Graph = graph
          Vertices = VerticesState []          
        }
        
    let s = runToCompletion state
    let result : float = s |> output (methodSum,0)
    Assert.AreEqual(0.0, result, "Execution result")