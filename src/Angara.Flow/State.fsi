/// State of an Angara flow.
///
/// DataFlowState compliments the dependency graph (module Angara.Graph)
/// to keep track of results of execution of the graph methods.
/// The state contains both status information and methods outputs.
module Angara.State

open Angara.Graph
open Angara.Data

/// Each dataflow vertex has a non-negative rank, determined by its input edges.
/// The vertex state is a result of multiple vertex executions,
/// represented as multidimensional map from vertex index to execution result state, `'vs`.
type MdVertexState<'vs> = MdMap<int, 'vs>
/// A key in the multidimensional vertex state.
type VertexIndex = int list

/// Keeps state of vertices.
type DataFlowState<'v, 'vs when 'v : comparison and 'v :> IVertex> =
    Map<'v, MdVertexState<'vs>>



