module StateOperations

open StateMachineMockup
open StateMachineMockup.StateMachine
open Vertices
open GraphExpression
open NUnit.Framework
open Angara.Data
open Angara.Graph

let internal exprToGraph = System.Collections.Generic.Dictionary<Graph, Angara.Graph.DataFlowGraph<Vertex> * Map<string, Vertex>>()

let vector (statuses : VertexStatus list) =
    statuses |> Seq.mapi(fun i v -> i,v) |> Seq.fold (fun map (i,v) -> MdMap.add [i] v map) MdMap.empty


let mdState (graphExpr:Graph, statuses:(string* MdMap<int,VertexStatus>) list, timeIndex : TimeIndex) =
    let flowGraph, nameToVertex = 
        match exprToGraph.TryGetValue graphExpr with
        | true, p -> p
        | false, _ ->
            let p = buildDag graphExpr
            exprToGraph.Add(graphExpr, p)
            p
        
    let flowState : Map<Vertex, MdMap<int,VertexState>> = 
        statuses 
        |> List.fold(fun map (nodeName, status) -> 
            let status2 = status |> MdMap.map(fun st -> { Status = st })
            map.Add(nameToVertex.[nodeName], status2)) Map.empty
    { Graph = flowGraph; FlowState = flowState; TimeIndex = timeIndex }, nameToVertex

let state (graphExpr:Graph, statuses:(string*VertexStatus) list, timeIndex : TimeIndex) =
    let mstatuses = statuses |> List.map(fun (n,s) -> n, MdMap.scalar s)
    mdState (graphExpr, mstatuses, timeIndex)


let start (nodeName : string) (state : State, nameToVertex: Map<string, Vertex>) =
    state |> transition (Message.Start (nameToVertex.[nodeName], [])), nameToVertex

let mdStart (nodeName : string, i : VertexIndex) (state : State, nameToVertex: Map<string, Vertex>) =
    state |> transition (Message.Start (nameToVertex.[nodeName], i)), nameToVertex

let iteration (nodeName : string) (startTime : TimeIndex) (state : State, nameToVertex: Map<string, Vertex>) =
    state |> transition (Message.Iteration (nameToVertex.[nodeName], [], [0], startTime)), nameToVertex

let iteration_array (nodeName : string) (startTime : TimeIndex) (shape : int list) (state : State, nameToVertex: Map<string, Vertex>) =
    state |> transition (Message.Iteration (nameToVertex.[nodeName], [], shape, startTime)), nameToVertex
    
let succeeded (nodeName : string) (startTime : TimeIndex) (state : State, nameToVertex: Map<string, Vertex>) =
    state |> transition (Message.Succeeded (nameToVertex.[nodeName], [], startTime)), nameToVertex

let stop (nodeName : string) (state : State, nameToVertex: Map<string, Vertex>) =
    state |> transition (Message.Stop (nameToVertex.[nodeName], [])), nameToVertex


let check (stateExp : State, nameToVertexExp: Map<string, Vertex>) (stateAct : State, nameToVertexAct: Map<string, Vertex>) =
    Assert.AreEqual(nameToVertexExp, nameToVertexAct, sprintf "Different names (%d)" stateExp.TimeIndex)
    Assert.AreEqual(stateExp.Graph, stateAct.Graph, sprintf "Different graphs (%d)" stateExp.TimeIndex)
    
    let exp = stateExp.FlowState |> Map.toArray
    let act = stateAct.FlowState |> Map.toArray
    Assert.AreEqual(exp.Length, act.Length, sprintf "Different length of states (%d)" stateExp.TimeIndex)
    Array.iter2(fun (v,e) (u,a) -> 
        Assert.AreEqual(v :> obj, u :> obj, sprintf "Different vertices of states (%d)" stateExp.TimeIndex)
        let eq = MdMap.equal(fun i s t -> Assert.AreEqual(s :> obj, t, sprintf "Different states at index (%A) (%d)" i stateExp.TimeIndex); true) e a
        Assert.IsTrue(eq, sprintf "Different shapes of %A states (%d)" v stateExp.TimeIndex)) exp act

    Assert.AreEqual(stateExp.TimeIndex, stateAct.TimeIndex, sprintf "Different time indices (%d)" stateExp.TimeIndex)
    (stateAct, nameToVertexAct)

