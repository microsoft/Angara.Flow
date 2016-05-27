module Angara.Trace

open System.Diagnostics

let Graph = new TraceSource("Angara.Graph")
let StateMachine = new TraceSource("Angara.StateMachine")

let Runtime = new TraceSource("Angara.Runtime")
module RuntimeId =
    let Evaluation = 2
    let SchedulerExecution = 3

type Event = TraceEventType

