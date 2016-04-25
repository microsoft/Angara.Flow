module StateMachineTests

open FsUnit
open NUnit.Framework
open Angara
open System
open Angara.Graph
open Angara.StateMachine

type Vertex(id: int, inps: Type list, outs: Type list) =
    member x.Id = id
    interface IVertex with
        member x.Inputs = inps
        member x.Outputs = outs
    interface IComparable with
        member x.CompareTo(other) = 
            match other with
            | :? Vertex as v -> id.CompareTo(v.Id)
            | _ -> invalidArg "other" "cannot compare values of different types"
    override x.Equals(other) =
        match other with
        | :? Vertex as y -> x.Id = y.Id
        | _ -> false
    override x.GetHashCode() = x.Id.GetHashCode()
        
type Data =
    interface IVertexData with
        member x.Contains(outRef) = failwith "Not implemented yet"
        member x.TryGetShape(outRef) = failwith "Not implemented yet"
        

[<Test; Category("CI")>]
[<Timeout(1000)>]
let ``Normalize adds a proper status when it is missing``() =
    let v1 = Vertex(1, [], [typeof<int>])
    let v2 = Vertex(2, [typeof<int>], [typeof<int>])
    let g1 = DataFlowGraph<Vertex>().Add(v1).Add(v2).Connect (v2,0) (v1,0)
    let s1 = Map.empty
    
    let s2, ch = StateMachine.normalize (g1, s1)
    Assert.AreEqual(2, s2.Count, "Number of states")
    let vs1 = s2.[v1].AsScalar()
    Assert.IsNull(vs1.Data)
    Assert.IsTrue(match vs1.Status with VertexStatus.CanStart t when t = 0u -> true | _ -> false)
    let vs2 = s2.[v2].AsScalar()
    Assert.IsNull(vs2.Data)
    Assert.IsTrue(match vs2.Status with VertexStatus.Incomplete (OutdatedInputs) -> true | _ -> false)

