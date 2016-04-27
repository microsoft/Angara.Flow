module Angara.Execution

open Angara.Graph
open Angara.StateMachine

/// Represents an element of a method output.
/// Angara stores method outputs in a flow state and passes them as other methods arguments.
type Artefact = obj


//////////////////////////////////////////////
// 
// Method Evaluator
//
//////////////////////////////////////////////

/// Keeps callback functions allowing a method to signal about its life cycle.
type EvaluationCallback<'d> = 
    { OnSucceeded       : 'd -> unit
    ; OnIteration       : 'd -> unit
    ; OnNoMoreIterations: unit -> unit
    ; OnFailure         : System.Exception -> unit 
    ; OnProgress        : System.IProgress<float> }

/// Keeps a vertex whose method should be evaluated as well as inputs and context for the method evaluation.
type EvaluationTarget<'v,'d when 'v:comparison and 'v:>IVertex and 'd:>IVertexData> = 
    { Vertex        : 'v
    ; Slice         : VertexIndex
    ; Data          : 'd
    ; Inputs        : Artefact[]
    ; Snapshot      : State<'v,'d> }

/// Represents a type which can evaluate a method.
[<Interface>]
type IMethodEvaluator<'v,'d when 'v:comparison and 'v:>IVertex and 'd:>IVertexData> =
    abstract Start: EvaluationCallback<'d> -> EvaluationTarget<'v,'d> -> unit
    

//////////////////////////////////////////////
// 
// Runtime
//
//////////////////////////////////////////////

open System

/// Represents an object which evaluates methods and produces messages for a `StateMachine`
/// those signal about method evaluation sucess or failure.
/// Also exposes information on method evaluation progress.
[<Interface>]
type IRuntime<'v,'d when 'v:comparison and 'v:>IVertex> =        
    inherit IDisposable

    /// Push-based notification about method evaluation success or failure.
    abstract Evaluation : IObservable<Message<'v,'d>>

    /// Allows to see progress of method evaluation.
    abstract Progress : IObservable<'v*VertexIndex*float>

val run : source:IObservable<State<'v,'d> * Changes<'v,'d>> -> IRuntime<'v,'d>
    

/// Represents a state which allows a resumable method to start not from the beginning.
type ResumableState = obj

/// Method is a reusable routine for model specification, simulation, transformation or analysis.
type Method<'v,'d when 'v:comparison and 'v:>IVertex> =
    | Function of (Artefact list -> Artefact list)
    | Iterative of (Artefact list -> Artefact list seq)
    | Resumable of ((Artefact list * ResumableState option) option * Artefact array  -> (Artefact list * ResumableState option) seq) 
    | Compound of (Artefact list -> Artefact list * State<'v,'d>)
    
/// A graph vertex which has an associated method.
[<Interface>]
type IExecutableVertex<'v, 'd when 'v:comparison and 'v:>IExecutableVertex<'v,'d>> =
    inherit IVertex
    abstract Method : Method<'v,'d>    

/// Represents output artefacts of a method.
/// E.g. an artefact can be missing if the dataflow snapshot couldn't be restored completely.
type Output = 
    | Full    of Artefact list
    | Partial of (Artefact option) list

/// Keeps method evaluation state: output artefacts, inner state, trace information.
[<Class>] 
type ExecutableVertexData<'v when 'v:comparison and 'v:>IExecutableVertex<'v,ExecutableVertexData<'v>>> =
    interface IVertexData

    /// Keeps output artefacts of the vertex. Can be missing.
    member Output: Output
    /// Keeps a resumable state that allows to continue execution of the vertex method.
    member ResumableState: ResumableState option
    /// If the data is an output of a compound method evaluation,
    /// this property keeps the final state of the compound method.
    member CompoundState: State<'v,ExecutableVertexData<'v>> option        