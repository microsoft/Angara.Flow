/// State of an Angara flow.
///
/// WorkState compliments the dependency graph (module Angara.Graph)
/// to keep track of results of execution of the graph methods.
/// The state contains both status information and methods outputs.
module Angara.State

open Angara.Graph
open Angara.Data

type VertexState<'vs> = MdMap<int, 'vs>

type DataFlowState<'v, 'vs when 'v : comparison and 'v :> IVertex> =
    Map<'v, VertexState<'vs>>

// A set of new states of updated or added or removed vertices.
type VertexChanges = 
    | New of VertexState
    | Removed 
    /// Shape of a vertex state could be changed.
    | ShapeChanged of old:VertexState * current: VertexState * isConnectionChanged:bool 
    /// Some items of a vertex state are changed.
    | Modified of indices:Set<VectorIndex> * old:VertexState * current: VertexState * isConnectionChanged:bool 

    
type Changes<'v when 'v : comparison> = Map<'v, VertexChanges>

