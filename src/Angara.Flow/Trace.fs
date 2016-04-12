module Angara.Trace

open System.Diagnostics

let Graph = new TraceSource("Angara.Graph")
let StateMachine = new TraceSource("Angara.StateMachine")


type Event = TraceEventType