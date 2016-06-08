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


//------------------------------------------------------------------------------------------
//
//    Transient state
//
//------------------------------------------------------------------------------------------

[<Test>]
let ``*Fo -> Fi, reproducing final artefact``() = 
    let g = graph {
        let! a = node0 "a"
        return! node1 "b" a
    }

    state (g, ["a", VertexStatus.Final_MissingOutputOnly; "b", VertexStatus.Final_MissingInputOnly], 0UL)
    |> start "a"
    |> equals (state (g, ["a", VertexStatus.Reproduces 1UL; "b", VertexStatus.Final_MissingInputOnly], 1UL))
    |> iteration "a"
    |> equals (state (g, ["a", VertexStatus.Reproduces 1UL; "b", VertexStatus.Final_MissingInputOnly], 2UL))
    |> succeeded "a"
    |> equals (state (g, ["a", VertexStatus.Final; "b", VertexStatus.Final], 3UL))
    |> ignore


[<Test>]
let ``Fo -> *Fio, reproducing final artefact with final transient input ``() = 
    let g = graph {
        let! a = node0 "a"
        return! node1 "b" a
    }

    state (g, ["a", VertexStatus.Final_MissingOutputOnly; "b", VertexStatus.Final_MissingInputOutput], 0UL)
    |> start "b"
    |> equals (state (g, ["a", VertexStatus.Reproduces 1UL; "b", VertexStatus.ReproduceRequested], 1UL))
    |> iteration "a"
    |> equals (state (g, ["a", VertexStatus.Reproduces 1UL; "b", VertexStatus.ReproduceRequested], 2UL))
    |> succeeded "a"
    |> equals (state (g, ["a", VertexStatus.Final; "b", VertexStatus.Reproduces 3UL], 3UL))
    |> iteration "b"
    |> equals (state (g, ["a", VertexStatus.Final; "b", VertexStatus.Reproduces 3UL], 4UL))
    |> succeeded "b"
    |> equals (state (g, ["a", VertexStatus.Final; "b", VertexStatus.Final], 5UL))
    |> ignore

[<Test>]
let ``Po -> *Pi -> F, restarting paused vertex with transient paused input``() = 
    let g = graph {
        let! a = node0 "a"
        let! b = node1 "b" a
        return! node1 "c" b
    }

    state (g, ["a", VertexStatus.Paused_MissingOutputOnly; "b", VertexStatus.Paused_MissingInputOnly; "c", VertexStatus.Final], 0UL)
    |> start "b"
    |> equals (state (g, ["a", VertexStatus.Started 1UL; "b", VertexStatus.Incomplete IncompleteReason.TransientInputs; "c", VertexStatus.Incomplete IncompleteReason.OutdatedInputs], 1UL))
    |> ignore

[<Test>]
let ``Fo -> *Pi -> F, restarting paused vertex with transient final input``() = 
    let g = graph {
        let! a = node0 "a"
        let! b = node1 "b" a
        return! node1 "c" b
    }

    state (g, ["a", VertexStatus.Final_MissingOutputOnly; "b", VertexStatus.Paused_MissingInputOnly; "c", VertexStatus.Final], 0UL)
    |> start "b"
    |> equals (state (g, ["a", VertexStatus.Reproduces 1UL; "b", VertexStatus.Incomplete IncompleteReason.TransientInputs; "c", VertexStatus.Incomplete IncompleteReason.OutdatedInputs], 1UL))
    |> succeeded "a"
    |> equals (state (g, ["a", VertexStatus.Final; "b", VertexStatus.CanStart 2UL; "c", VertexStatus.Incomplete IncompleteReason.OutdatedInputs], 2UL))
    |> ignore

[<Test>]
let ``P -> *Po -> F, restarting paused vertex with transient output``() = 
    let g = graph {
        let! a = node0 "a"
        let! b = node1 "b" a
        return! node1 "c" b
    }

    state (g, ["a", VertexStatus.Paused; "b", VertexStatus.Paused_MissingOutputOnly; "c", VertexStatus.Final], 0UL)
    |> start "b"
    |> equals (state (g, ["a", VertexStatus.Paused; "b", VertexStatus.Started 1UL; "c", VertexStatus.Incomplete IncompleteReason.OutdatedInputs], 1UL))
    |> ignore


[<Test>]
let ``Po -> Fio -> *Fio -> Fi, restarting final vertex with transient input having transient paused vertex upstream ``() = 
    let g = graph {
        let! a = node0 "a"
        let! b = node1 "b" a
        let! c = node1 "c" b
        return! node1 "d" c
    }

    state (g, ["a", VertexStatus.Paused_MissingOutputOnly; "b", VertexStatus.Final_MissingInputOutput; "c", VertexStatus.Final_MissingInputOutput; "d", VertexStatus.Final_MissingInputOnly], 0UL)
    |> start "c"
    |> equals (state (g, ["a", VertexStatus.Started 1UL; "b", VertexStatus.Incomplete IncompleteReason.OutdatedInputs; "c", VertexStatus.Incomplete IncompleteReason.OutdatedInputs; "d", VertexStatus.Incomplete IncompleteReason.OutdatedInputs], 1UL))
    |> ignore