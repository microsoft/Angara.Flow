namespace Angara

open System
open Angara.Graph
open Angara.StateMachine
open Angara.Execution

[<Class>]
type Flow(graph : DataFlowGraph<Method>, state : DataFlowState<Method,VertexState<MethodVertexData>>, scheduler : Scheduler) =
    inherit Engine(graph, state, scheduler)