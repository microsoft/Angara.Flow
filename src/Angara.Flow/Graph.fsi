/// Angara dependency graph. 
///
/// Vertices of the graph represent Angara methods. 
/// Edges describe where arguments of a method come from and where the results of the method go.
///
/// Each graph vertex has a fixed number of typed inputs corresponding to method arguments
/// and a fixed number of typed outputs corresponding to the method result type.
/// All the other details an Angara method invocation are in the Execution module.
/// When adding an edge the graph checks that input type of the edge target is assignable
/// from the output type of the edge source.
///
/// Array types have special meaning to the graph.
/// See comments to the ConnectionType discriminated union.
module Angara.Graph

[<Interface>]
type IEdge<'v> =
    abstract member Source : 'v
    abstract member Target : 'v

type DirectedAcyclicGraph<'v,'e when 'v : comparison and 'e :> IEdge<'v> and 'e : equality> =
    new : unit -> DirectedAcyclicGraph<'v,'e>
    new : vertices:Map<'v,'e list> -> DirectedAcyclicGraph<'v,'e>
    
    member internal VertexToEdges : Map<'v,'e list>
    member Vertices : Set<'v>

    /// Returns a sequence of edges that are incident into the given vertex.
    member InEdges : v:'v -> seq<'e>
    /// Returns a sequence of edges that are incident out of the given vertex.
    member OutEdges : v:'v -> seq<'e>
    /// Returns a DAG that is a maximum subgraph of the current graph such that
    /// (i) it contains all vertices that are connected with the given vertex as the terminal vertex;
    /// (ii) for each of its vertices, it contains all edges of the current graph that are part of 
    /// any existing directed path between the selected vertex and the given vertex. 
    member Provenance : v:'v -> DirectedAcyclicGraph<'v,'e>
    /// Returns a sequence of vertices which are connected as initial vertices with the given vertex.
    /// The given vertex is first in the returned sequence; order of the rest sequence is undefined.
    member EnumerateDownstream : v:'v -> seq<'v>

    member AddEdge : e:'e -> DirectedAcyclicGraph<'v,'e>
    member AddVertex : v:'v -> DirectedAcyclicGraph<'v,'e>
    member TryRemoveVertex : v:'v -> DirectedAcyclicGraph<'v,'e> option
    member RemoveEdge : e:'e -> DirectedAcyclicGraph<'v,'e>

    /// Returns a new graph which contains all vertices and edges of the two original graphs.
    /// Method fails if the given graphs have same vertex.
    member Combine : target:DirectedAcyclicGraph<'v,'e> -> DirectedAcyclicGraph<'v,'e>
    
    member Map : vertex:('v -> 'v2) -> edge:('e * 'v2 * 'v2 -> 'e2) -> DirectedAcyclicGraph<'v2,'e2>
                when 'v2 : comparison and 'e2 :> IEdge<'v2> and 'e2 : equality  
    /// Calls the given function for each vertex starting from 'source' and finishing with either sink vertices or vertices for each
    /// the given function returns `false`.
    member Fold : source:'v -> fold:('a -> 'v -> 'a * bool) -> state:'a -> 'a    
    /// Folds graph in topological order so vertices without incoming edges go first,
    /// then go vertices that has incoming edges from already folded vertices and so on
    /// Folder function takes state, vertex and outgoing edges for this vertex.
    member TopoFold : folder:('s -> 'v -> 'e list -> 's) -> s0:'s -> 's

    
val find : f:('v -> bool) -> startVertex:'v -> g:DirectedAcyclicGraph<'v,'e> -> 'v option
    when 'v : comparison and 'e :> IEdge<'v> and 'e : equality
/// Returns a sequence of vertices which starts with given vertex and finishes when isFinal returns true (final vertices are not included).
val toSeqSubgraph : isFinal:('v -> Lazy<seq<'e>> -> Lazy<seq<'e>> -> bool) -> g:DirectedAcyclicGraph<'v,'e> -> sources:seq<'v> -> seq<'v>
    when 'v : comparison and 'e :> IEdge<'v> and 'e : equality
val topoSort : g:DirectedAcyclicGraph<'v,'e> -> selection:seq<'v> -> seq<'v> 
    when 'v : comparison and 'e :> IEdge<'v> and 'e : equality
val getSources : g:DirectedAcyclicGraph<'v,'e> -> seq<'v>
    when 'v : comparison and 'e :> IEdge<'v> and 'e : equality
val toSeqSorted : g:DirectedAcyclicGraph<'v,'e> -> 'v list
    when 'v : comparison and 'e :> IEdge<'v> and 'e : equality
val isReachableFrom : target:'v -> source:'v -> g:DirectedAcyclicGraph<'v,'e> -> bool
    when 'v : comparison and 'e :> IEdge<'v> and 'e : equality

/////////////////////////////////////////////////////
//
// More specific DataFlowGraph. 
//
// A Vertex may have multiple inputs and outputs.
// An edge connects certain output with certain input.
// Array inputs accept multiple connections.
// Array outputs may increase a rank of the downstream subgraph.
//
/////////////////////////////////////////////////////

/// A graph vertex has a fixed number of typed inputs corresponding to method arguments
/// and a fixed number of typed outputs corresponding to the method result type.
[<Interface>]
type IVertex =
    abstract member Inputs : System.Type []
    abstract member Outputs : System.Type []

/// zero-based index into an array of method inputs
type InputRef = int
type InputPort<'v> = 'v * InputRef

/// zero-based index into an array of method outputs
type OutputRef = int
type OutputPort<'v> = 'v * OutputRef

type ConnectionType =
    /// The edge passes an artefact from the edge source to the edge target as-is.
    /// The input type must be assignable from the output type.
    | OneToOne of rank: int
    /// The edge induces multiple evaluation of the edge target for each element of the artefact array.
    | Scatter of rank: int
    /// The edge collects an input array from multiple results of the edge source evaluations.
    | Reduce of rank: int
    /// The edge means that edge input's artefact is an individual element of an array that will be provided as an input to the edge target.
    /// The `index` is a zero-based index into an array of a method input.
    /// There can be multiple `Collect` edges going to an input port having different `index` value.
    | Collect of index: int * rank: int

type Edge<'v> =
    interface IEdge<'v>
    new : source:OutputPort<'v> * target:InputPort<'v> * connType:ConnectionType -> Edge<'v>
    member InputRef : InputRef
    member OutputRef : OutputRef
    member Source : 'v
    member Target : 'v
    member Type : ConnectionType

val edgeRank : e:Edge<'a> -> int
val vertexRank : v:'v -> graph:DirectedAcyclicGraph<'v,Edge<'v>> -> int when 'v : comparison

type DataFlowGraph<'v when 'v : comparison and 'v :> IVertex> =
    new : unit -> DataFlowGraph<'v>
    new : graph:DirectedAcyclicGraph<'v,Edge<'v>> -> DataFlowGraph<'v>

    member Structure : DirectedAcyclicGraph<'v,Edge<'v>>
    
    member Add : vertex:'v -> DataFlowGraph<'v>
    member BatchAlter : batchDisconnect:('v * InputRef * 'v * OutputRef) list ->
                        batchConnect:('v * InputRef * 'v * OutputRef) list ->
                        DataFlowGraph<'v>
    member Connect : vTrg:'v * pTrg:InputRef -> vSrc:'v * pSrc:OutputRef -> DataFlowGraph<'v>
    member ConnectItem : vTrg:'v * pTrg:InputRef -> vSrc:'v * pSrc:OutputRef -> DataFlowGraph<'v>
    member Disconnect : target:'v * inpRef:InputRef -> DataFlowGraph<'v>
    member Disconnect : target:'v * inpRef:InputRef * source:'v *
                    outputRef:OutputRef -> DataFlowGraph<'v>
    member TryRemove : v:'v -> DataFlowGraph<'v> option

    static member CreateFrom : methods:(int * 'v) list ->
                        connections:((int * OutputRef) * (int * InputRef)) list ->
                        DataFlowGraph<'v> * Map<int,'v>
    static member Empty : DataFlowGraph<'v>
