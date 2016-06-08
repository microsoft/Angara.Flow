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
    |> equals (state (g, ["a", VertexStatus.Started 1UL; "b", VertexStatus.Incomplete IncompleteReason.OutdatedInputs], 1UL))
    |> iteration "a"
    |> equals (state (g, ["a", VertexStatus.Continues 1UL; "b", VertexStatus.CanStart 2UL], 2UL))
    |> succeeded "a"
    |> equals (state (g, ["a", VertexStatus.Final; "b", VertexStatus.CanStart 2UL], 3UL))
    |> ignore



[<Test>]
let ``a -> b, iterations of 'a' restart 'b'``() = 
    let g = graph {
        let! a = node0 "a"
        return! node1 "b" a
    }

    state (g, ["a", VertexStatus.CanStart 0UL; "b", VertexStatus.Incomplete IncompleteReason.OutdatedInputs], 0UL)
    |> start "a"
    |> equals (state (g, ["a", VertexStatus.Started 1UL; "b", VertexStatus.Incomplete IncompleteReason.OutdatedInputs], 1UL))
    |> iteration "a"
    |> equals (state (g, ["a", VertexStatus.Continues 1UL; "b", VertexStatus.CanStart 2UL], 2UL))
    |> iteration "a"                                            
    |> equals (state (g, ["a", VertexStatus.Continues 1UL; "b", VertexStatus.CanStart 2UL], 3UL))
    |> start "b"                                                
    |> equals (state (g, ["a", VertexStatus.Continues 1UL; "b", VertexStatus.Started 4UL], 4UL))
    |> iteration "a"                                            
    |> equals (state (g, ["a", VertexStatus.Continues 1UL; "b", VertexStatus.CanStart 5UL], 5UL))
    |> succeeded "a"
    |> equals (state (g, ["a", VertexStatus.Final; "b" ,VertexStatus.CanStart 5UL], 6UL))
    |> ignore

[<Test>]
let ``a -> b -> c, iteration of 'a' properly invalidates downstream``() = 
    let g = graph {
        let! a = node0 "a"
        let! b = node1 "b" a
        return! node1 "c" b
    }

    state (g, ["a", VertexStatus.Continues 0UL; "b", VertexStatus.Continues 0UL; "c", VertexStatus.Continues 0UL], 0UL)
    |> iteration "a"
    |> equals (state (g, ["a", VertexStatus.Continues 0UL; "b", VertexStatus.CanStart 1UL; "c", VertexStatus.Incomplete IncompleteReason.OutdatedInputs], 1UL))
    |> ignore