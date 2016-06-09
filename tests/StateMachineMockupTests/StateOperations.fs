module StateOperations

open StateMachineMockup
open StateMachineMockup.StateMachine
open Vertices
open GraphExpression
open NUnit.Framework


let internal exprToGraph = System.Collections.Generic.Dictionary<Graph, Angara.Graph.DataFlowGraph<Vertex> * Map<string, Vertex>>()

let state (graphExpr:Graph, statuses:(string*VertexStatus) list, timeIndex : TimeIndex) =
    let flowGraph, nameToVertex = 
        match exprToGraph.TryGetValue graphExpr with
        | true, p -> p
        | false, _ ->
            let p = buildDag graphExpr
            exprToGraph.Add(graphExpr, p)
            p
        
    let flowState : Map<Vertex, VertexState> = 
        statuses 
        |> List.fold(fun map (nodeName, status) -> map.Add(nameToVertex.[nodeName], { Status = status })) Map.empty
    { Graph = flowGraph; FlowState = flowState; TimeIndex = timeIndex }, nameToVertex



let start (nodeName : string) (state : State, nameToVertex: Map<string, Vertex>) =
    state |> transition (Message.Start (nameToVertex.[nodeName])), nameToVertex

let iteration (nodeName : string) (startTime : TimeIndex) (state : State, nameToVertex: Map<string, Vertex>) =
    state |> transition (Message.Iteration (nameToVertex.[nodeName], startTime)), nameToVertex
    
let succeeded (nodeName : string) (startTime : TimeIndex) (state : State, nameToVertex: Map<string, Vertex>) =
    state |> transition (Message.Succeeded (nameToVertex.[nodeName], startTime)), nameToVertex

let stop (nodeName : string) (state : State, nameToVertex: Map<string, Vertex>) =
    state |> transition (Message.Stop (nameToVertex.[nodeName])), nameToVertex


let equals (stateExp : State, nameToVertexExp: Map<string, Vertex>) (stateAct : State, nameToVertexAct: Map<string, Vertex>) =
    Assert.AreEqual(nameToVertexExp, nameToVertexAct, sprintf "Different names (%d)" stateExp.TimeIndex)
    Assert.AreEqual(stateExp.Graph, stateAct.Graph, sprintf "Different graphs (%d)" stateExp.TimeIndex)
    Assert.AreEqual(stateExp.FlowState, stateAct.FlowState, sprintf "Different states (%d)" stateExp.TimeIndex)
    Assert.AreEqual(stateExp.TimeIndex, stateAct.TimeIndex, sprintf "Different time indices (%d)" stateExp.TimeIndex)
    (stateAct, nameToVertexAct)

