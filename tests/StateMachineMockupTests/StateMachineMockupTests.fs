module StateMachineMockupTests


open NUnit.Framework
open StateMachineMockup
open StateMachineMockup.StateMachine
open Vertices
open GraphExpression
open StateOperations


[<Test>]
let ``a -> b, transitions start -> iteration -> succeeded``() = 
    let g = graph {
        let! a = node0 "a"
        return! node1 "b" a
    }

    state (g, ["a", VertexStatus.CanStart 0UL; "b", VertexStatus.Incomplete IncompleteReason.OutdatedInputs], 0UL)
    |> start "a"
    |> equals (state (g, ["a", VertexStatus.Started 1UL; "b",VertexStatus.Incomplete IncompleteReason.OutdatedInputs], 1UL))
    |> iteration "a"
    |> equals (state (g, ["a", VertexStatus.Continues 1UL; "b",VertexStatus.CanStart 2UL], 2UL))
    |> succeeded "a"
    |> equals (state (g, ["a", VertexStatus.Final; "b",VertexStatus.CanStart 2UL], 3UL))
    |> ignore

[<Test>]
let ``a -> b, iterations of a restart b``() = 
    let g = graph {
        let! a = node0 "a"
        return! node1 "b" a
    }

    state (g, ["a", VertexStatus.CanStart 0UL; "b", VertexStatus.Incomplete IncompleteReason.OutdatedInputs], 0UL)
    |> start "a"
    |> equals (state (g, ["a", VertexStatus.Started 1UL; "b",VertexStatus.Incomplete IncompleteReason.OutdatedInputs], 1UL))
    |> iteration "a"
    |> equals (state (g, ["a", VertexStatus.Continues 1UL; "b",VertexStatus.CanStart 2UL], 2UL))
    |> iteration "a"
    |> equals (state (g, ["a", VertexStatus.Continues 1UL; "b",VertexStatus.CanStart 2UL], 3UL))
    |> succeeded "a"
    |> equals (state (g, ["a", VertexStatus.Final; "b",VertexStatus.CanStart 2UL], 4UL))
    |> ignore