module GraphExpression


type Vertex(name : string, inputs, outputs) = 
    
    member x.Name = name
    member x.Inputs : System.Type list = inputs
    member x.Outputs : System.Type list = outputs

    interface Angara.Graph.IVertex with
        member x.Inputs : System.Type list = inputs
        member x.Outputs : System.Type list = outputs

    interface System.IComparable with
        member x.CompareTo(other : obj) =
            match other with
            | null -> 1
            | :? Vertex as v -> name.CompareTo v.Name
            | _ -> raise (new System.ArgumentException("Object is not a Vertex"))

    override x.ToString() = sprintf "Vertex '%s'" name

type Node =
    { Name : string
      Inputs : (Node*int) list
      OutputRank : int }

type Graph =
    { Nodes : Node list 
      Target : Node option }

type GraphBuilder() =
    
    member x.Bind(expr: Graph, follow: Node -> Graph) : Graph =
        let followGraph = follow (expr.Target.Value)
        let allNodes = expr.Nodes @ followGraph.Nodes |> List.distinct
        { Nodes = allNodes; Target = followGraph.Target }

    member x.ReturnFrom(expr: Graph) = expr

    member x.Return(_:unit) = { Nodes = []; Target = None }

let graph = GraphBuilder()

let node1 name inNodes =
    let node = { Name = name; Inputs = inNodes |> List.map(fun n -> n,0); OutputRank = 0 }
    { Nodes = node :: inNodes; Target = Some node}

let scatter_node1 name inNodes =
    let node = { Name = name; Inputs = inNodes |> List.map(fun n -> n,0); OutputRank = 1 }
    { Nodes = node :: inNodes; Target = Some node}

let reduce_node1 name inNode =
    let node = { Name = name; Inputs = [inNode,1]; OutputRank = 0 }
    { Nodes = [node; inNode]; Target = Some node}


type internal Dag = Angara.Graph.DataFlowGraph<Vertex>

let rec internal buildType rank =
    if rank = 0 then typeof<int>
    else System.Array.CreateInstance(buildType (rank-1), 0).GetType()

let buildDag (expr: Graph) =
    let rec addMethod (dag : Dag) (node : Node) (nodeToVertex : Map<string,Vertex>) =
        match nodeToVertex |> Map.containsKey node.Name with
        | true -> dag, nodeToVertex
        | false ->
            let v = Vertex(node.Name, node.Inputs |> List.map(fun (_,inputRank) -> buildType inputRank), [buildType node.OutputRank])
            let dag2 = dag.Add v 
            let nodeToVertex2 = nodeToVertex.Add(node.Name, v)
            node.Inputs 
            |> Seq.mapi(fun i inp -> (i,inp))
            |> Seq.fold(fun (dag, nodeToVertex) (inpRef, (input,_)) ->
                let dag3, nodeToVertex3 = addMethod dag input nodeToVertex
                (dag3.Connect (v, inpRef) (nodeToVertex3.[input.Name], 0)), nodeToVertex3) (dag2, nodeToVertex2)

    let dag, nodeToVertex = 
        expr.Nodes 
        |> List.rev
        |> Seq.fold(fun (dag:Dag, nodeToVertex:Map<string, Vertex>) node -> addMethod dag node nodeToVertex) (Dag(), Map.empty<string, Vertex>)

    dag, nodeToVertex