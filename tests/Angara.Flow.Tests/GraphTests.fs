module ``Directed Acyclic Graph operations``

open FsUnit
open NUnit.Framework
open Angara
open System

type Trace = System.Diagnostics.Trace
type IntEdge(source, target) = 
    interface Angara.Graph.IEdge<int> with
        member __.Source = source
        member __.Target = target

type IntDag = Angara.Graph.DirectedAcyclicGraph<int,IntEdge>

[<Test; Category("CI")>]
[<Timeout(1000)>]
let ``Graph vertices are folded in topological order``() = 
    // No vertices
    IntDag().TopoFold (fun s v e -> v :: s) [] |> should equal []
   
    // One vertex
    IntDag().AddVertex(1).TopoFold (fun s v e -> v :: s) [] |> should equal [1] 

    // Path of two vertices
    IntDag().AddVertex(1).AddVertex(2).AddEdge(IntEdge(2,1)).
        TopoFold (fun s v e -> v :: s) [] |> should equal [1; 2]  // List is generated in reverse order
    
    // Path of three vertices
    IntDag().AddVertex(1).AddVertex(2).AddVertex(3).AddEdge(IntEdge(3,1)).AddEdge(IntEdge(1,2)).
        TopoFold (fun s v e -> v :: s) [] |> should equal [2; 1; 3]  // List is generated in reverse order

    // Four vertices in two layers (1,3) and (2,4)
    let result = IntDag().AddVertex(1).AddVertex(2).AddVertex(3).AddVertex(4).
                         AddEdge(IntEdge(1,2)).AddEdge(IntEdge(1,4)).
                         AddEdge(IntEdge(3,2)).AddEdge(IntEdge(3,4)).
                         TopoFold (fun s v e -> v :: s) []
    Assert.AreEqual(result.Length,4)
    Assert.IsTrue(result.[0] = 2 && result.[1] = 4 || result.[0] = 4 && result.[1] = 2)
    Assert.IsTrue(result.[2] = 1 && result.[3] = 3 || result.[2] = 3 && result.[3] = 1)

[<Test; Category("CI")>]
[<Timeout(1000)>]
[<ExpectedException(typeof<InvalidOperationException>)>]
let ``No loops are allowed``() = 
    let g = IntDag().AddVertex(1).AddVertex(2).AddEdge(IntEdge(1,2))
    g.AddEdge(IntEdge(2,1)) |> ignore


