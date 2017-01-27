module ``Engine tests``

open NUnit.Framework

open Angara.Graph
open Angara.States
open Angara.Execution
open Angara.Data
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

    override x.Execute(_, checkpoint) =
        let iters, i0 = 
            match checkpoint with
            | Some i -> values |> Seq.skip (unbox i), unbox i
            | None -> values, 0
        iters |> Seq.mapi(fun i v -> [unbox v], unbox (i+i0))


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

    (Observable.FirstAsync(states, new Func<StateUpdate<Method,MethodOutput>, bool>(isFinal)) |> Observable.map(fun update -> update.State)).GetAwaiter()
    

let output (m:Method, output:OutputRef) (state:State<Method,MethodOutput>) =
    unbox (state.Vertices.[m].AsScalar().Data.Value.Artefacts.TryGet(output).Value)

let runToCompletion state =
    use engine = new Engine(state, Scheduler.ThreadPool())
    let final = pickFinal engine.Changes   
    engine.Start()
    final.GetResult()

//--------------------------------------------------------------------------------------------------------------------
// Tests


[<Test; Category("CI")>]
let ``Engine executes a flow containing a single method`` () =
    let methodOne : Method = upcast MethodMakeValue System.Math.PI
    let graph = 
        FlowGraph.empty
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
        FlowGraph.empty
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
        FlowGraph.empty
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
                Some (vs.AsScalar().Data.Value.Artefacts.TryGet(0).Value)
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
        FlowGraph.empty
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
        FlowGraph.empty
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

[<Test; Category("CI")>]
let ``Engine allows to pause an executing iterative method``() =
    let iters = Seq.initInfinite (fun i -> System.Threading.Thread.Sleep(10); float(i))
    let methodIter : Method = upcast MethodMakeValueIter<float> iters

    let graph = 
        FlowGraph.empty
        |> FlowGraph.add methodIter

    let state =
        { TimeIndex = 0UL
          Graph = graph
          Vertices = VerticesState [] }


    use engine = new Engine(state, Scheduler.ThreadPool())
    engine.Start()

    let s5 = engine.Changes.Take(10).GetAwaiter().GetResult()
    match s5.State.Vertices.[methodIter].AsScalar().Status with
    | VertexStatus.Continues _ -> () // ok
    | _ -> Assert.Fail "Unexpected status"

    let finish = 
        engine.Changes.FirstAsync(fun update ->
            match update.State.Vertices.[methodIter].AsScalar().Status with
            | VertexStatus.Paused _ -> true
            | _ -> false)
            .GetAwaiter()

    engine.Post( Messages.Stop { Vertex = methodIter; Index = VertexIndex.Empty })

    let s = finish.GetResult()

    Assert.LessOrEqual(1.0, s.State |> output (methodIter, 0), "Output")

[<Test; Category("CI")>]
let ``Engine allows to continue a paused iterative method and a checkpoint enables to start from last iteration`` () =
    let iters = [| for i in 0..10 -> float(i) * System.Math.PI |]
    let methodIter : Method = upcast MethodMakeValueIter<float> iters
    let methodInc : Method = upcast MethodOp<float,float> (fun a -> a + 1.0)

    let graph =
        FlowGraph.empty
        |> FlowGraph.add methodIter
        |> FlowGraph.add methodInc
        |> FlowGraph.connect (methodIter,0) (methodInc,0)


    // We create an initial state where the method `methodIter` is paused after `k` iterations with corresponding checkpoint.
    let k = 3 
    let state = 
        { TimeIndex = 0UL
          Graph = graph
          Vertices = [methodIter, MdMap.scalar( { Status = VertexStatus.Paused [0]; Data = Some(MethodOutput.Full([iters.[k]],Some (box k))) }) ] |> Map.ofList
        }

    use engine = new Engine(state, Scheduler.ThreadPool())
    let final = pickFinal engine.Changes 
    let iterCount = 
       engine.Changes
        .TakeWhile(fun update -> // We stop watching for changes when the `methodIter` is completed.
            match update.State.Vertices.[methodIter].AsScalar().Status with 
            | VertexStatus.Final _ -> false
            | _ -> true)
        .Count(fun update -> // Counting number of iterations performed by `methodIter` since it will be resumed.
            match update.Changes |> Map.tryFind methodIter with
            | Some (Modified(_,oldvs,vs,_)) ->
                match oldvs.AsScalar().Status,vs.AsScalar().Status with
                | VertexStatus.Paused _, VertexStatus.Continues _ -> false // this transition indicates that the method is started but not yet produced any output
                | VertexStatus.Continues _, VertexStatus.Continues _ -> true // indicates that new iteration output produced
                | _ -> false
            | _ -> false)
        .GetAwaiter()

    engine.Start()

    // We resume the paused `methodIter` using message `Start`.
    // The method status will transit to `Continues` and it will be executed using the checkpoint.
    // `MethodMakeValueIter.Execute` will use the checkpoint to start from the last iteration but not from beginning.
    engine.Post (Messages.Start { Vertex = methodIter; Index = VertexIndex.Empty; CanStartTime = None })

    let s = final.GetResult()

    let result : float = s |> output (methodInc,0)
    Assert.AreEqual(Array.last iters + 1.0, result, "Execution result")
    Assert.AreEqual(iters.Length - k, iterCount.GetResult(),  "Number of iterations performed")


[<Test; Category("CI")>]
let ``Engine allows to cancel execution`` () = 
    Assert.Inconclusive("Incomplete");