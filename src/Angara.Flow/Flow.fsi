namespace Angara

open System
open Angara.Graph
open Angara.StateMachine
open Angara.Execution

[<Class>]
type Flow =
    inherit Engine

    new : DataFlowGraph<Method> * DataFlowState<Method,VertexState<MethodVertexData>> * Scheduler -> Flow

