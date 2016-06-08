module GraphExpression

type Node =
    { Name : string
      Inputs : Node list }

type Graph =
    { Nodes : Node list 
      Target : Node option }

type GraphBuilder() =
    
    member x.Bind(expr: Graph, follow: Node -> Graph) : Graph =
        let followGraph = follow (expr.Target.Value)
        followGraph

    member x.ReturnFrom(expr: Graph) = expr

    member x.Return(_:unit) = { Nodes = []; Target = None }

let graph = GraphBuilder()

let node0 name =
    let node = { Name = name; Inputs = [] }
    { Nodes = [node]; Target = Some node}

let node1 name inNode =
    let node = { Name = name; Inputs = [inNode] }
    { Nodes = [node; inNode]; Target = Some node}

type internal Vertex = StateMachineMockup.Vertex
type internal Dag = Angara.Graph.DataFlowGraph<Vertex>

let buildDag (expr: Graph) =
    let dag, nodeToVertex = expr.Nodes |> Seq.fold(fun (dag:Dag, nodeToVertex:Map<string, Vertex>) node -> 
        let v = Vertex(node.Name, node.Inputs |> List.map(fun _ -> typeof<int>), [typeof<int>])
        dag.Add v, nodeToVertex.Add(node.Name, v)) (Dag(), Map.empty<string, Vertex>)

    let dag = expr.Nodes |> Seq.fold(fun (dag:Dag) node -> 
        node.Inputs 
        |> Seq.mapi(fun i inp -> i,inp) 
        |> Seq.fold(fun dag (inpRef, input: Node) -> dag.Connect (nodeToVertex.[node.Name], inpRef) (nodeToVertex.[input.Name], 0)) dag) dag
    
    dag, nodeToVertex