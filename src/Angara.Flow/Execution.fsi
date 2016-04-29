module Angara.Execution

open Angara.Graph
open Angara.StateMachine

/// Represents an element of a method output.
/// Angara stores method outputs in a flow state and passes them as other methods arguments.
type Artefact = obj


//////////////////////////////////////////////
// 
// Runtime
//
//////////////////////////////////////////////

open System

/// Represents an object which evaluates methods and produces messages for a `StateMachine`.
/// These messages signal about method evaluation sucess or failure.
/// Also exposes information on method evaluation progress.
type Runtime<'v,'d when 'v:comparison and 'v:>IVertex> =        
    inherit IDisposable

    /// Push-based notification about method evaluation success or failure.
    abstract Evaluation : IObservable<Message<'v,'d>>

    /// Allows to see progress of method evaluation.
    abstract Progress : IObservable<'v*VertexIndex*float>

/// Represents an engine which maintains a combination of the StateMachine and Runtime.
/// It makes the changes flow from the state machine to the runtime and
/// makes the messages flow from the runtime to the state machine.
type Engine<'v,'d when 'v:comparison and 'v:>IVertex and 'd:>IVertexData> =
    inherit IDisposable
    abstract StateMachine : StateMachine<'v,'d>
    abstract Runtime : Runtime<'v,'d>
    
    /// Starts the engine, if it is suspended; otherwise, fails.
    abstract Start : unit -> unit
    
/// A graph vertex which has an associated method.
type Method<'internalMethodData> =
    inherit IVertex
    abstract Execute : Artefact list * 'internalMethodData option -> Artefact list * 'internalMethodData option

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
    member InternalData : 'internalMethodData