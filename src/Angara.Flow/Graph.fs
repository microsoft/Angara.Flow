module Angara.Graph

type private HashSet<'a> = System.Collections.Generic.HashSet<'a>
type private Queue<'a> = System.Collections.Generic.Queue<'a>

/////////////////////////////////////////////////////
//
// Abstract DirectedAcyclicGraph
//
/////////////////////////////////////////////////////

type IEdge<'v> =
    abstract member Source : 'v with get
    abstract member Target : 'v with get

[<System.Diagnostics.DebuggerDisplayAttribute("Vertices: {Vertices}")>]
type DirectedAcyclicGraph<'v,'e when 'v : comparison and 'e :> IEdge<'v> and 'e : equality>(vertices : Map<'v, 'e list>) =    
    let vertices : Map<'v, 'e list> = vertices
    let mutable cacheInEdges = Map.empty<'v, 'e seq>

    new() = DirectedAcyclicGraph<'v,'e>(Map.empty)
        
    member internal this.VertexToEdges = vertices

    member this.Vertices with get() = vertices |> Map.toSeq |> Seq.map fst |> Set.ofSeq

    member this.AddEdge (e : 'e) = 
            if e.Source = e.Target then failwith "Source and target nodes are same"
            if not (Map.containsKey e.Source vertices) then failwith "Source node is not found" 
            if not (Map.containsKey e.Target vertices) then failwith "Target node is not found" 

            // Check that no loop is created by the new edge:
            let source = e.Source
            let visited = ref Set.empty<'v>
            let queue = Queue<'v>()
            queue.Enqueue(e.Target)
            while queue.Count > 0 do
                let v = queue.Dequeue()
                if v = source then invalidOp "Cannot add the edge because this would make a graph cyclic"
                visited := (!visited).Add v
                vertices.[v] |> List.iter (fun e -> if not ((!visited).Contains e.Target) then queue.Enqueue(e.Target))

            let edges = e :: vertices.[e.Source] 
            DirectedAcyclicGraph<'v,'e>(vertices.Add (e.Source, edges)) 

    member this.AddVertex v = DirectedAcyclicGraph<'v,'e>(vertices.Add(v, List.empty))
    
    /// Removes a vertex which has no outgoing edges.
    /// If vertex is not found, does nothing.
    /// If vertex has outgoing edges, the method returns None.
    /// Its in edges are removed automatically.
    member this.TryRemoveVertex v =
        match vertices |> Map.tryFind v with 
            | Some(edges)   -> 
                if edges.IsEmpty then 
                    let inedges = this.InEdges v
                    let vertices = inedges |> Seq.fold(fun (vertices:Map<'v,'e list>) (inedge:'e) ->
                        let outedges = vertices.[inedge.Source]
                        let outedges = outedges |> List.remove inedge
                        vertices |> Map.add inedge.Source outedges) vertices
                    Some(DirectedAcyclicGraph<'v,'e>(vertices.Remove v)) 
                else None // "Cannot remove a vertex because it has outgoing edges"
            | None          -> Some(this)

    member this.RemoveEdge (e:'e) =
        let vertices = match vertices |> Map.tryFind e.Source with 
                        | Some(edges)   -> vertices.Add (e.Source, edges |> List.remove e)
                        | None          -> vertices
        DirectedAcyclicGraph<'v,'e>(vertices)
    
    member this.OutEdges v = 
        match v |> vertices.TryFind with
        | Some edges -> edges :> seq<'e>
        | None -> failwith "Vertex not found in a graph"

    member this.InEdges v =
            match cacheInEdges.TryFind v with
            | Some inedges -> inedges
            | None ->
                let edges = Map.toSeq vertices |> Seq.map snd |> Seq.concat
                let inedges = edges |> Seq.filter (fun edge -> edge.Target = v) |> Seq.cache
                cacheInEdges <- cacheInEdges |> Map.add v inedges
                inedges

    

    /// Folds graph in topological order so vertices without incoming edges go first,
    /// then go vertices that has incoming edges from already folded vertices and so on
    /// Folder function takes state, vertex and outgoing edges for this vertex.
    /// Fold method uses Kahn algorithm.
    member this.TopoFold<'s> (folder:'s -> 'v -> 'e list -> 's) (s0:'s) =
            // Build mutable dictionary of indegree
            let indeg = new System.Collections.Generic.Dictionary<'v,int>()
            vertices |> Map.iter (fun v _ -> indeg.Add(v,0))
            vertices |> Map.iter (fun v edges -> edges |> List.iter (fun e -> indeg.[e.Target] <- indeg.[e.Target] + 1))

            // Fill the queue with vertices with zero indegree
            let queue = new System.Collections.Generic.Queue<'v>();
            vertices |> Map.iter (fun v _ -> if indeg.[v] = 0 then queue.Enqueue(v))

            // Enumerate all vertices
            let mutable s = s0
            while queue.Count > 0 do
                let v = queue.Dequeue()
                s <- folder s v vertices.[v]
                vertices.[v] |> List.iter (fun e -> let count = indeg.[e.Target]
                                                    indeg.[e.Target] <- count - 1
                                                    if count = 1 then queue.Enqueue(e.Target))
            s

    /// Gets a provenance DAG for the given vertex (it is a subset of the current DAG).
    /// Includes the target vertex as well.
    member this.Provenance (v:'v) =
        // todo: not efficient algorithm can be replaced with search from source to downstream vertices using DFS.
        let rec inVertices (v:'v) = 
            seq { yield v;
                  yield! v |> this.InEdges |> Seq.map(fun (e:'e) -> inVertices e.Source) |> Seq.concat }
        let vv = inVertices v |> Set.ofSeq
        let vertices =
            vertices |> Map.toSeq |> Seq.choose(fun (v, edges) ->
                match vv.Contains v with
                | false -> None
                | true -> Some(v, edges |> List.filter(fun (e:'e) -> e.Target |> vv.Contains))) 
            |> Map.ofSeq
        DirectedAcyclicGraph(vertices)

    /// Enumerates downstream vertices.
    /// Each vertex (including the given one which goes first) is enumerated once in an undefined order.
    member this.EnumerateDownstream (v: 'v) : 'v seq =
        let visited = new HashSet<'v>()
        let queue = new Queue<'v>()
        queue.Enqueue(v)
        seq {
            while(queue.Count > 0) do
                let v = queue.Dequeue()
                yield v                
                visited.Add(v) |> ignore
                this.OutEdges(v) |> Seq.iter(fun e -> if not (visited.Contains(e.Target)) then queue.Enqueue(e.Target))
        }

    /// Doesn't allow target to have same vertices as in the original graph.
    member this.Combine (target: DirectedAcyclicGraph<'v,'e>) : DirectedAcyclicGraph<'v,'e> =
        let source = this.VertexToEdges
        let target = target.VertexToEdges
        let result = target |> Map.fold(fun (graph:Map<'v,'e list>) (v:'v) (vedges:'e list) -> 
            if graph.ContainsKey v then failwith "Cannot combine two graphs because the share a vertex"
            else
                graph.Add(v, vedges)) source
        DirectedAcyclicGraph(result)

    /// Calls 'fold' for each vertex starting from 'source' downstream to end vertices or final vertices.
    /// A vertex is final if 'fold' returns "false" for it.
    member this.Fold<'a> (source:'v) (fold: 'a->'v->'a*bool) (state: 'a) : 'a =
        let mutable state = state
        let visited = new HashSet<'v>()
        let queue = new Queue<'v>()
        queue.Enqueue(source)
        while(queue.Count > 0) do
            let v = queue.Dequeue()
            visited.Add(v) |> ignore
            let s, notFinal = fold state v
            state <- s
            if notFinal then 
                this.OutEdges(v) |> Seq.iter(fun e -> if not (visited.Contains(e.Target)) then queue.Enqueue(e.Target))
        state

    member this.Map<'v2,'e2 when 'v2 : comparison and 'e2 :> IEdge<'v2> and 'e2 : equality> (vertex: 'v -> 'v2) (edge: 'e*'v2*'v2->'e2): DirectedAcyclicGraph<'v2,'e2> = 
        let oldToNew : Map<'v,'v2> = vertices |> Map.map(fun (v:'v) _ -> vertex(v))
        let m : Map<'v2,'e2 list> = vertices |> Map.toSeq |> Seq.map (fun (v:'v, edges: 'e list) -> 
            let sv = oldToNew.[v] 
            sv, edges |> List.map(fun (e:'e) -> edge (e, sv, oldToNew.[e.Target]))) |> Map.ofSeq
        DirectedAcyclicGraph(m)
    
let find (f: 'v -> bool) (startVertex:'v) (g: DirectedAcyclicGraph<'v,'e>)  : 'v option =
    let visited = new HashSet<'v>()
    let queue = new Queue<'v>()
    let found = ref None
    queue.Enqueue(startVertex)

    while(queue.Count > 0) do
        let v = queue.Dequeue()
        visited.Add(v) |> ignore
        if f v then
            found := Some v
            queue.Clear()
        else
            v |> g.OutEdges |> Seq.iter(fun e -> if not (visited.Contains(e.Target)) then queue.Enqueue(e.Target))
    !found

/// Returns a sequence of vertices which starts with given vertex and finishes when isFinal returns true,
/// and final vertices separately.
/// isFinal: vertex -> lazy<input edges> -> lazy<output edges> -> is final
/// Returns: the subgraph vertices and the final vertices
let toSeqSubgraphAndFinal (isFinal: 'v -> Lazy<'e seq> -> Lazy<'e seq> -> bool) (g: DirectedAcyclicGraph<'v,'e>) (sources:'v seq) : 'v seq * 'v seq =
    let visited = new HashSet<'v>()
    let queue = new Queue<'v>()
    let final = new HashSet<'v>()
    sources |> Seq.iter(fun v -> queue.Enqueue(v))
    let subvertices =
        seq {
            while(queue.Count > 0) do
                let v = queue.Dequeue()
                visited.Add(v) |> ignore
                let inedges = lazy( g.InEdges v )
                let outedges = lazy( g.OutEdges v )
                if not (isFinal v inedges outedges) then
                    yield v                
                    outedges.Value |> Seq.iter(fun e -> if not (visited.Contains(e.Target)) then queue.Enqueue(e.Target))
                else
                    final.Add(v) |> ignore
        }
    subvertices, upcast final

/// Returns a sequence of vertices which starts with given vertex and finishes when isFinal returns true (final vertices are not included).
/// isFinal: vertex -> lazy<input edges> -> lazy<output edges> -> is final
let toSeqSubgraph (isFinal: 'v -> Lazy<'e seq> -> Lazy<'e seq> -> bool) (g: DirectedAcyclicGraph<'v,'e>) (sources:'v seq) : 'v seq =
    let visited = new HashSet<'v>()
    let queue = new Queue<'v>()
    sources |> Seq.iter(fun v -> queue.Enqueue(v))
    seq {
        while(queue.Count > 0) do
            let v = queue.Dequeue()
            visited.Add(v) |> ignore
            let inedges = lazy( g.InEdges v )
            let outedges = lazy( g.OutEdges v )
            if not (isFinal v inedges outedges) then
                yield v                
                outedges.Value |> Seq.iter(fun e -> if not (visited.Contains(e.Target)) then queue.Enqueue(e.Target))
    }

let topoSort (g: DirectedAcyclicGraph<'v,'e>) (selection: 'v seq) : 'v seq = 
    let vertexToEdges = g.VertexToEdges
    let selection = selection |> Seq.toList
    if selection.Length > 0 then
        let selectionSet = selection |> Set.ofList

        // Build mutable dictionary of indegree
        let indeg = new System.Collections.Generic.Dictionary<'v,int>()
        selection |> Seq.iter (fun v -> if not (indeg.ContainsKey(v)) then indeg.Add(v,0))
        // counts in-edges excluding source vertices from outside of the selection:
        vertexToEdges |> Map.iter (fun v edges -> 
            if selectionSet.Contains v then
                edges |> Seq.iter (fun e -> if selectionSet.Contains e.Target then indeg.[e.Target] <- indeg.[e.Target] + 1))

        // Fill the queue with vertices with zero indegree
        let queue = new System.Collections.Generic.Queue<'v>();
        selection |> Seq.iter (fun v -> if indeg.[v] = 0 then queue.Enqueue(v))

        // Enumerate all vertices
        seq {
            while queue.Count > 0 do
                let v = queue.Dequeue()
                yield v
                vertexToEdges.[v] |> List.iter (fun e ->                             
                    match indeg.ContainsKey e.Target with
                    | true ->
                        let count = indeg.[e.Target]
                        indeg.[e.Target] <- count - 1
                        if count = 1 then queue.Enqueue(e.Target)
                    | false -> ())
        }
    else Seq.empty

let getSources (g: DirectedAcyclicGraph<'v,'e>) : 'v seq = 
    g.VertexToEdges |> Map.toSeq |> Seq.map fst |> Seq.filter(fun v -> g.InEdges v |> Seq.isEmpty)

let toSeqSorted (g: DirectedAcyclicGraph<'v,'e>) : 'v list = 
    g.TopoFold(fun acc v _ -> v :: acc) [] |> List.rev

let isReachableFrom (target: 'v) (source: 'v) (g: DirectedAcyclicGraph<'v,'e>) : bool =
    (g |> find (fun v -> v = target) source).IsSome


[<Interface>]
type IVertex =
    abstract member Inputs : System.Type list
    abstract member Outputs : System.Type list

type InputRef       = int
type InputPort<'v> = 'v * InputRef

type OutputRef      = int
type OutputPort<'v> = 'v * OutputRef

type ConnectionType = 
  | OneToOne    of rank: int  
  | Scatter     of rank: int
  | Reduce      of rank: int
  /// index is a zero-based index into an array of a method input
  | Collect     of index:int * rank: int 
  override this.ToString() =
        match this with
        | OneToOne s -> sprintf "one-to-one %O" s
        | Scatter s -> sprintf "scatter %O" s
        | Reduce s -> sprintf "reduce %O" s
        | Collect (i,s) -> sprintf "collect at %d %O" i s
  

type Edge<'v>(source: OutputPort<'v>, target: InputPort<'v>, connType:ConnectionType) =
    member this.Source with get(): 'v = fst source
    member this.Target with get(): 'v = fst target
    member this.OutputRef with get() = snd source
    member this.InputRef with get() = snd target
    member this.Type with get() = connType
    interface IEdge<'v> with
        member this.Source with get(): 'v = fst source
        member this.Target with get(): 'v = fst target

    override this.ToString() =
        sprintf "%O[%d] -- %O --> %O[%d]" (fst source) (snd source) connType (fst target) (snd target)

type private Dag<'v when 'v : comparison> = DirectedAcyclicGraph<'v,Edge<'v>>


let edgeRank (e:Edge<_>) =
    match e.Type with
    | OneToOne rank -> rank
    | Scatter rank -> rank
    | Reduce rank -> rank
    | Collect(_, rank) -> rank

let vertexRank (v:'v) (graph:Dag<'v>) : int =
    let ranks = graph.InEdges v |> Seq.map edgeRank
    if Seq.isEmpty ranks then 0 else Seq.max ranks


/// Represents a graph of methods where edge is a data dependency between two methods.
/// (Level 2)
type FlowGraph<'v when 'v : comparison and 'v :> IVertex> (graph:Dag<'v>) =

    static let isAssignableFrom (tSrc:System.Type) (tTrg:System.Type) = tTrg.IsAssignableFrom(tSrc)
    
    /// Returns rank>0, if t is an array (e.g. rank of 'a[][] is 2),
    /// returns 0 if t is not an array. 
    /// NB: returns 0 if t is a multidimensional (>1) array.
    static let jaggedRank (t:System.Type) = 
        let rec getRank rank (t:System.Type) = 
            match t.IsArray with
            | true -> 
                let r = t.GetArrayRank()
                if r > 1 then rank
                else t.GetElementType() |> getRank (rank+1)
            | false -> rank
        getRank 0 t
        

    /// Automatically chooses edge type from One-To-One, Scatter, Reduce and adds the edge.
    /// Returns graph having the added edge and the edge rank.
    let connect (target:InputPort<'v>) (source:OutputPort<'v>) (graph:Dag<'v>) : Dag<'v>*int =        
        let vSrc, pSrc = source
        let cSrc = vSrc.Outputs.[pSrc]
        let vTrg, pTrg = target
        let cTrg = vTrg.Inputs.[pTrg]
        let portConnections() = graph.InEdges vTrg |> Seq.filter(fun e -> e.InputRef = pTrg)

        let srcRank = graph |> vertexRank vSrc
        let edge,scope =
            match isAssignableFrom cSrc cTrg with
            | true -> // One-to-one edge
                if not (Seq.isEmpty (portConnections())) then
                    failwith (sprintf "Cannot connect %s[%d] to %s[%d] because there is an existing input connection" (vTrg.ToString()) pTrg (vSrc.ToString()) pSrc)
                Edge(source, target, OneToOne srcRank), srcRank
            | false -> 
                match jaggedRank cSrc, jaggedRank cTrg with
                | rankSrc, rankTrg when rankSrc = rankTrg+1 ->  
                    match isAssignableFrom (cSrc.GetElementType()) (cTrg) with
                    | true -> // Scatter edge
                        if not (Seq.isEmpty (portConnections())) then
                            failwith (sprintf "Cannot connect %s[%d] to %s[%d] because there is an existing input connection" (vTrg.ToString()) pTrg (vSrc.ToString()) pSrc)
                        let rank = srcRank+1
                        Edge(source, target, Scatter rank), rank
                    | false -> failwith (sprintf "Cannot connect %s[%d] to %s[%d] because the types are not assignable" (vTrg.ToString()) pTrg (vSrc.ToString()) pSrc)
              
                | rankSrc, rankTrg when rankSrc+1 = rankTrg -> // Reduce edge
                    match isAssignableFrom (cSrc) (cTrg.GetElementType()) with
                    | true -> 
                        if srcRank < 1 then
                            failwith(sprintf "Cannot connect %s[%d] to %s[%d] as reduce because the source vertex is not a vector" (vTrg.ToString()) pTrg (vSrc.ToString()) pSrc)
                        if not (Seq.isEmpty (portConnections())) then
                            failwith (sprintf "Cannot connect %s[%d] to %s[%d] because there is an existing input connection" (vTrg.ToString()) pTrg (vSrc.ToString()) pSrc)
                        let rank = srcRank - 1
                        Edge(source, target, Reduce rank), rank
                    | false -> failwith (sprintf "Cannot connect %s[%d] to %s[%d] because the types are not assignable" (vTrg.ToString()) pTrg (vSrc.ToString()) pSrc)
              
                | _ -> failwith (sprintf "Cannot connect %s[%d] to %s[%d] because the types are not assignable" (vTrg.ToString()) pTrg (vSrc.ToString()) pSrc)    
        graph.AddEdge(edge), scope

    /// Adds new edge of type "collect".
    /// Returns graph having the added edge and the edge rank.
    let connectItemAt (index:int option) (target:InputPort<'v>) (source:OutputPort<'v>) (graph:Dag<'v>) : Dag<'v>*int =
        let vSrc,pSrc = source
        let cSrc = vSrc.Outputs.[pSrc]
        let vTrg,pTrg = target
        let cTrg = vTrg.Inputs.[pTrg]
        let portConnections() = graph.InEdges vTrg |> Seq.filter(fun e -> e.InputRef = pTrg)

        let srcRank = graph |> vertexRank(vSrc)
        let edge,scope =
            match jaggedRank cSrc, jaggedRank cTrg with              
            | rankSrc, rankTrg when rankSrc+1 = rankTrg ->
                match isAssignableFrom (cSrc) (cTrg.GetElementType()) with
                | true -> // Collect edge
                    let index = 
                        match index with
                        | Some index -> 
                            if portConnections() |> Seq.exists (fun e -> match e.Type with Collect(j,_) -> j = index | _ -> true) then
                                failwith (sprintf "Cannot connect %s[%d] to %s[%d] at index %d because there is an existing connection" (vTrg.ToString()) pTrg (vSrc.ToString()) pSrc index)
                            else Some(index)
                        | None -> portConnections() |> 
                                    Seq.fold(fun ind e -> match ind, e.Type with Some(i), Collect(j,_) -> Some(if i<=j then j+1 else i) | _,_ -> None) (Some 0)  
                    match index with
                    | None ->
                        failwith (sprintf "Cannot connect %s[%d] to %s[%d] because there are existing connections other than Collect" (vTrg.ToString()) pTrg (vSrc.ToString()) pSrc)
                    | Some(index) -> 
                        Edge(source, target, Collect(index,srcRank)), srcRank
                | false ->  failwith (sprintf "Cannot connect %s[%d] to %s[%d] as a collection item because the types are not assignable" (vTrg.ToString()) pTrg (vSrc.ToString()) pSrc)
              
            | _ -> failwith (sprintf "Cannot connect %s[%d] to %s[%d] as a collection item because the array ranks are incorrect" (vTrg.ToString()) pTrg (vSrc.ToString()) pSrc)    
        graph.AddEdge(edge), srcRank

    /// Adds new edge of type "collect".
    /// Returns graph having the added edge and the edge rank.
    let connectItem (target:InputPort<'v>) (source:OutputPort<'v>) (graph:Dag<'v>) : Dag<'v>*int =
        connectItemAt None target source graph
                
    let updateDownstreamRanks vTrg (graph:Dag<'v>) = 
        let mutable graph = graph
        let mutable edges = List<Edge<'v>>.Empty // keeps the removed & restored edges 

        // Removing all existing edges downstream
        let visited = new HashSet<'v>()
        let toVisit = new Queue<'v>()
        toVisit.Enqueue(vTrg)
        while toVisit.Count > 0 do
            let v = toVisit.Dequeue()
            if visited.Add(v) then
                let downstream = graph.OutEdges v |> Seq.toList
                downstream |> List.iter(fun e -> toVisit.Enqueue(e.Target)) 
                edges <- edges @ downstream 
                graph <- downstream |> List.fold(fun graph e -> graph.RemoveEdge(e)) graph
                    
        // Restoring edges 
        graph <- edges |> List.fold(fun graph e -> 
            let graph,rank = 
                match e.Type with
                | Collect (index,_) -> connectItemAt (Some index) (e.Target,e.InputRef) (e.Source,e.OutputRef) graph
                | _ -> connect (e.Target,e.InputRef) (e.Source,e.OutputRef) graph
            graph) graph
        graph

    let updateDownstreamRanks (vTrg:'v list) (graph:Dag<'v>) = 
        let downstream = vTrg 
                            |> toSeqSubgraph(fun _ _ _ -> false) graph 
                            |> topoSort graph
                            |> Seq.toList
        // Removing all existing edges downstream
        let graphR,edges = downstream |> List.fold(fun (graph:Dag<'v>,acc) v -> graph.OutEdges v |> Seq.fold(fun (g,acc) e -> g.RemoveEdge e, e :: acc) (graph,acc)) (graph,[])

        // Restoring edges
        let restore (graph:Dag<'v>) (e:Edge<'v>) = 
            let graph,_ = 
                    match e.Type with
                    | Collect (index,_) -> connectItemAt (Some index) (e.Target,e.InputRef) (e.Source,e.OutputRef) graph
                    | _ -> connect (e.Target,e.InputRef) (e.Source,e.OutputRef) graph
            graph
        let graphN = edges |> List.rev |> List.fold restore graphR
        graphN

    let connectWithAdjusting (target:InputPort<'v>) (source:OutputPort<'v>) (connect: InputPort<'v>->OutputPort<'v>->Dag<'v>->Dag<'v>*int) (graph:Dag<'v>) : Dag<'v> =
        let vTrg,_ = target
        let rankOrig = vertexRank vTrg graph
        let graph,rank = connect target source graph
        if rankOrig <> rank then updateDownstreamRanks [vTrg] graph else graph
            
    let disconnectWithRenumber (remove:Edge<'v>) (graph:Dag<'v>) = 
        let graph = 
            match remove.Type with
            | Collect (idx,_) -> // renumber remaining "one-to-many" connections of the input
                let edges = graph.InEdges remove.Target |> Seq.filter (fun e -> 
                    let j = match e.Type with Collect (j,s) -> j | _ -> failwith "Unexpected type of edge"
                    e.InputRef = remove.InputRef && j > idx)                                                                                     
                let number edges = edges |> Seq.map (fun (e:Edge<'v>) -> 
                    let i,s = match e.Type with Collect (j,s) -> j-1,s | _ -> failwith "Unexpected type of edge"
                    e, Edge((e.Source,e.OutputRef),(e.Target,e.InputRef),Collect(i,s)))
                number edges |> Seq.fold (fun (g:Dag<'v>) (oldEdge,newEdge) -> g.RemoveEdge(oldEdge).AddEdge(newEdge)) graph                                                                       
            | _ -> graph
        graph.RemoveEdge(remove)
                    
    new() = FlowGraph(Dag())

    member this.Structure with get() = graph

    member this.Add vertex = FlowGraph(graph.AddVertex vertex)

    /// Removes a vertex which has no outgoing edges.
    /// If vertex is not found, does returns original graph.
    /// If vertex has outgoing edges, the method returns None.
    /// Its in edges are removed automatically.
    member this.TryRemove v =
        match graph.TryRemoveVertex v with
            | Some g -> Some(FlowGraph(g))
            | None -> None

    /// Connects two vertices using one-to-one, scatter or reduce connection types.
    member this.Connect (vTrg:'v, pTrg) (vSrc:'v, pSrc) =
        let graph = connectWithAdjusting (vTrg,pTrg) (vSrc,pSrc) connect graph
        FlowGraph(graph)

    /// Connects two vertices using collect connection type.
    member this.ConnectItem (vTrg:'v, pTrg) (vSrc:'v, pSrc) =
        let graph = connectWithAdjusting (vTrg,pTrg) (vSrc,pSrc) connectItem graph
        FlowGraph(graph)

    member this.Disconnect (target:'v, inpRef:InputRef) =
        let rank = vertexRank target graph
        let remove = target |> graph.InEdges |> Seq.filter (fun e -> e.InputRef = inpRef)                
        let ng = remove |> Seq.fold (fun (g:Dag<'v>) e -> g.RemoveEdge e) graph
        let ng = if rank > 0 then updateDownstreamRanks [target] ng else ng 
        FlowGraph(ng)

    member this.Disconnect (target:'v, inpRef, source:'v, outputRef) =
        let remove = source |> graph.OutEdges |> Seq.tryFind (fun e -> e.Target = target && e.InputRef = inpRef && e.OutputRef = outputRef)
        let ng = 
            match remove with
            | Some(remove) -> 
                let vrank = vertexRank target graph
                let graph = disconnectWithRenumber remove graph
                let vrank2 = vertexRank target graph
                if(vrank <> vrank2) then updateDownstreamRanks [target] graph else graph
            | None -> graph
        FlowGraph(ng)        


    member this.Vertices = graph.Vertices        

    member this.BatchAlter (batchDisconnect: ('v*InputRef*'v*OutputRef) list) (batchConnect: ('v*InputRef*'v*OutputRef) list) =
        let g1 : Dag<'v> = 
            batchDisconnect |> List.fold(fun graph (target, inpRef, source, outputRef) ->   
                let remove = source |> graph.OutEdges |> Seq.tryFind (fun e -> e.Target = target && e.InputRef = inpRef && e.OutputRef = outputRef)
                match remove with
                | Some(remove) -> disconnectWithRenumber remove graph
                | None -> graph) graph
        let g2 : Dag<'v> = 
            batchConnect |> List.fold(fun graph (target, inpRef, source, outputRef) -> connect (target, inpRef) (source, outputRef) graph |> fst) g1

        let g3 : Dag<'v> = 
            Seq.append (batchDisconnect |> Seq.map(fun (target, _, _, _) -> target)) (batchConnect |> Seq.map(fun (target, _, _, _) -> target))
            |> Seq.fold(fun graph target -> updateDownstreamRanks [target] graph) g2

        FlowGraph(g3)


    static member Empty = new FlowGraph<'v>()

    static member CreateFrom (methods:(int*'v) list) (connections:((int*OutputRef)*(int*InputRef)) list) =
        let (g,m) = methods |> Seq.fold (fun (g:FlowGraph<'v>,m:Map<int,'v>) (i,v) -> let g' = g.Add(v) in (g', m.Add(i, v))) (FlowGraph.Empty, Map.empty) 
        let f (g:FlowGraph<'v>) (src:(int*OutputRef), trg:(int*InputRef)) =
            let mt = m.[fst trg]
            let ms = m.[fst src]
            let tt = mt.Inputs.[snd trg]
            let ts = ms.Outputs.[snd src]
            let rankSrc, rankTrg = jaggedRank ts, jaggedRank tt
            if (rankSrc+1 = rankTrg) && (isAssignableFrom ts (tt.GetElementType())) then g.ConnectItem (mt, snd trg) (ms, snd src)
            else g.Connect (mt, snd trg) (ms, snd src)
        let g = connections |> Seq.fold f g
        (g, m)

module FlowGraph =
    let empty<'v when 'v : comparison and 'v :> IVertex> : FlowGraph<'v> = FlowGraph<'v>()
    let add (v:'v) (g:FlowGraph<'v>) : FlowGraph<'v> = g.Add v
    let connect (s,sp) (t,tp) (g:FlowGraph<'v>) : FlowGraph<'v> = g.Connect (t,tp) (s,sp)
        

open Angara.Data
open Angara.Option

type MdVertexState<'vs> = MdMap<int, 'vs>
type VertexIndex = int list

type VerticesState<'v, 'vs when 'v : comparison and 'v :> IVertex> = 
    Map<'v, MdVertexState<'vs>>

module DataFlowState =
    let tryGet (v,i) (s:VerticesState<'v,'vs>) =
        opt{
            let! vs = s |> Map.tryFind v
            return! vs |> MdMap.tryFind i
        }