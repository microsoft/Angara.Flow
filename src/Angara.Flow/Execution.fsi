module Angara.Execution

open System
open Angara.Graph
open Angara.StateMachine

/// Represents an element of a method output.
/// Angara stores method outputs in a flow state and passes them as other methods arguments.
type Artefact = obj

/// A graph vertex which can be executed.
[<AbstractClass>]
type Method<'internalMethodData> =
    interface IVertex
    interface IComparable
    abstract Execute : Artefact list * 'internalMethodData option -> (Artefact list * 'internalMethodData option) seq

/// Represents output artefacts of a method.
/// An artefact can be missing, e.g. if the dataflow snapshot couldn't be restored completely.
type Output = 
    | Full    of Artefact list
    | Partial of (Artefact option) list
    member TryGet : OutputRef -> Artefact option

/// Keeps method evaluation state: output artefacts, inner state, trace information.
[<Class>] 
type MethodVertexData<'internalMethodData> =
    interface IVertexData

    /// Keeps output artefacts of the vertex. Can be missing, e.g. because of serialization limitations.
    member Output : Output
    member InternalData : 'internalMethodData option

//////////////////////////////////////////////
// 
// Runtime
//
//////////////////////////////////////////////

[<Interface>]
type IScheduler =
    abstract Start : (unit -> unit) -> unit
    abstract Start : Async<unit> -> unit

/// Represents an object which evaluates methods and produces messages for a `StateMachine`.
/// These messages signal about method evaluation sucess or failure.
/// Also exposes information on method evaluation progress.
[<Sealed>]
type Runtime<'internalMethodData> =
    interface IDisposable

    /// Push-based notification about method evaluation success or failure.
    member Evaluation : IObservable<Message<Method<'internalMethodData>, MethodVertexData<'internalMethodData>>>

    /// Allows to see progress of method evaluation.
    member Progress : IObservable<Method<'internalMethodData>*VertexIndex*float>

/// Represents an engine which maintains a combination of the StateMachine and Runtime.
/// It makes the changes flow from the state machine to the runtime and
/// makes the messages flow from the runtime to the state machine.
[<Class>]
type Engine<'internalMethodData> =
    interface IDisposable

    member StateMachine : StateMachine<Method<'internalMethodData>, MethodVertexData<'internalMethodData>>
    member Runtime : Runtime<'internalMethodData>
    
    member Start : unit -> unit
    
