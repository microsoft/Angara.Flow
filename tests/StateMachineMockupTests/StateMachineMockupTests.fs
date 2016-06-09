module StateMachineMockupTests


open NUnit.Framework
open Vertices
open GraphExpression
open StateOperations


//------------------------------------------------------------------------------------------
//
//    Simple transitions
//
//------------------------------------------------------------------------------------------

[<Test>]
let ``Conducts simple method execution``() = 
    let g = graph {
        return! node0 "a" 
    }

                        state (g, ["a", CanStart 0UL], 0UL)
    |> start "a"
    |>          equals (state (g, ["a", Started 1UL], 1UL))
    |> iteration "a" 1UL
    |>          equals (state (g, ["a", Continues 1UL], 2UL))
    |> succeeded "a" 1UL
    |>          equals (state (g, ["a", Final], 3UL))
    |> ignore

[<Test>]
let ``Obsolete 'succeeded' message is ignored``() = 
    let g = graph {
        return! node0 "a" 
    }

                        state (g, ["a", CanStart 0UL], 0UL)
    |> start "a"
    |>          equals (state (g, ["a", Started 1UL], 1UL))
    |> iteration "a" 1UL
    |>          equals (state (g, ["a", Continues 1UL], 2UL))
    |> iteration "a" 0UL // <-- obsolete message
    |>          equals (state (g, ["a", Continues 1UL], 3UL)) // remains executing, without any exceptions
    |> succeeded "a" 0UL // <-- obsolete message
    |>          equals (state (g, ["a", Continues 1UL], 4UL)) // remains executing, without any exceptions
    |> ignore

[<Test>]
let ``Obsolete 'iteration' message is ignored``() = 
    let g = graph {
        return! node0 "a" 
    }

                        state (g, ["a", CanStart 0UL], 0UL)
    |> start "a"
    |>          equals (state (g, ["a", Started 1UL], 1UL))
    |> iteration "a" 0UL // <-- obsolete message
    |>          equals (state (g, ["a", Started 1UL], 2UL)) // remains started, without any exceptions
    |> ignore

[<Test>]
[<ExpectedException(typeof<System.InvalidOperationException>)>]
let ``Final method cannot be started``() = 
    let g = graph {
        return! node0 "a" 
    }

                        state (g, ["a", Final], 0UL)
    |> start "a"
    |> ignore

//------------------------------------------------------------------------------------------
//
//   2-methods transitions
//
//------------------------------------------------------------------------------------------

[<Test>]
let ``Method iteration enables its downstream method to start``() = 
    let g = graph {
        let! a = node0 "a"
        return! node1 "b" a
    }

                        state (g, ["a", CanStart 0UL; "b", Incomplete OutdatedInputs], 0UL)
    |> start "a"
    |>          equals (state (g, ["a", Started 1UL; "b", Incomplete OutdatedInputs], 1UL))
    |> iteration "a" 1UL
    |>          equals (state (g, ["a", Continues 1UL; "b", CanStart 2UL], 2UL))
    |> succeeded "a" 1UL
    |>          equals (state (g, ["a", Final; "b", CanStart 2UL], 3UL))
    |> ignore



[<Test>]
let ``Next method iteration cancels the possible execution of its downstream method``() = 
    let g = graph {
        let! a = node0 "a"
        return! node1 "b" a
    }

                        state (g, ["a", CanStart 0UL; "b", Incomplete OutdatedInputs], 0UL)
    |> start "a"
    |>          equals (state (g, ["a", Started 1UL; "b", Incomplete OutdatedInputs], 1UL))
    |> iteration "a" 1UL
    |>          equals (state (g, ["a", Continues 1UL; "b", CanStart 2UL], 2UL))
    |> iteration "a" 1UL                                            
    |>          equals (state (g, ["a", Continues 1UL; "b", CanStart 2UL], 3UL))
    |> start "b"                                                
    |>          equals (state (g, ["a", Continues 1UL; "b", Started 4UL], 4UL))    
    |> iteration "a" 1UL // <-- cancels the execution of "b"; possible obsolete "iteration" from "b" will be ignored.
    |>          equals (state (g, ["a", Continues 1UL; "b", CanStart 5UL], 5UL))
    |> succeeded "a" 1UL
    |>          equals (state (g, ["a", Final; "b" ,CanStart 5UL], 6UL))
    |> ignore


[<Test>]
let ``Method iteration cancels the execution of the immediate downstream method and makes other downstream methods incomplete``() = 
    let g = graph {
        let! a = node0 "a"
        let! b = node1 "b" a
        return! node1 "c" b
    }

                        state (g, ["a", Continues 0UL; "b", Continues 0UL; "c", Continues 0UL], 0UL)
    |> iteration "a" 1UL
    |>          equals (state (g, ["a", Continues 0UL; "b", CanStart 1UL; "c", Incomplete OutdatedInputs], 1UL))
    |> ignore


[<Test>]
let ``Method iterations can be paused then resumed``() = 
    let g = graph {
        let! a = node0 "a"
        return! node1 "b" a
    }

    state (g, ["a", Continues 1UL; "b", Started 4UL], 4UL)
    |> stop "a"                                            
    |> equals (state (g, ["a", Paused; "b", Started 4UL], 5UL))
    |> iteration "b" 4UL
    |> equals (state (g, ["a", Paused; "b", Continues 4UL], 6UL))
    |> succeeded "b" 4UL
    |> equals (state (g, ["a", Paused; "b", Final], 7UL))
    |> start "a"
    |> equals (state (g, ["a", Continues 8UL; "b", Final], 8UL))
    |> iteration "a" 8UL
    |> equals (state (g, ["a", Continues 8UL; "b", CanStart 9UL], 9UL))
    |> ignore


//------------------------------------------------------------------------------------------
//
//    Transient state
//
//------------------------------------------------------------------------------------------

[<Test>]
let ``Conducts through reproduction of a final transient artefact after reinstate``() = 
    // *Fo -> Fi
    let g = graph {
        let! a = node0 "a"
        return! node1 "b" a
    }

                        state (g, ["a", Final_MissingOutputOnly; "b", Final_MissingInputOnly], 0UL)
    |> start "a"
    |>          equals (state (g, ["a", Reproduces 1UL; "b", Final_MissingInputOnly], 1UL))
    |> iteration "a" 1UL
    |>          equals (state (g, ["a", Reproduces 1UL; "b", Final_MissingInputOnly], 2UL))
    |> succeeded "a" 0UL // <-- obsolete message
    |>          equals (state (g, ["a", Reproduces 1UL; "b", Final_MissingInputOnly], 3UL))
    |> succeeded "a" 1UL
    |>          equals (state (g, ["a", Final; "b", Final], 4UL))
    |> ignore


[<Test>]
let ``If a final transient method depends on another final transient method, its reproduction forces reproduction of the upstream method``() = 
    // Fo -> *Fio
    let g = graph {
        let! a = node0 "a"
        return! node1 "b" a
    }

    state (g, ["a", Final_MissingOutputOnly; "b", Final_MissingInputOutput], 0UL)
    |> start "b" // <-- reproduction of "b" first requires reproduction of "a" in order to get inputs for "b"
    |> equals (state (g, ["a", Reproduces 1UL; "b", ReproduceRequested], 1UL))
    |> iteration "a" 1UL
    |> equals (state (g, ["a", Reproduces 1UL; "b", ReproduceRequested], 2UL))
    |> succeeded "a" 1UL // <-- when "a" is reproduced, "b" can be reproduced as well
    |> equals (state (g, ["a", Final; "b", Reproduces 3UL], 3UL))
    |> iteration "b" 3UL
    |> equals (state (g, ["a", Final; "b", Reproduces 3UL], 4UL))
    |> succeeded "b" 3UL
    |> equals (state (g, ["a", Final; "b", Final], 5UL))
    |> ignore

[<Test>]
let ``If a paused transient method depends on another paused transient method, its execution forces re-execution of the upstream method``() = 
    // The transient methods cannot be a result of state machine actions.
    // The only origin of the transiency is deserialization of a state, if some artefacts cannot be serialized/deserialized.
    // The paused method's outputs cannot be reproduced, therefore the "start" makes them to execute and invalidats downstream methods,
    // which were computed for the already missing value.
    
    let g = graph { // Po -> *Pi -> F
        let! a = node0 "a"
        let! b = node1 "b" a
        return! node1 "c" b
    }

    state (g, ["a", Paused_MissingOutputOnly; "b", Paused_MissingInputOnly; "c", Final], 0UL)
    |> start "b"
    |> equals (state (g, ["a", Started 1UL; "b", Incomplete TransientInputs; "c", Incomplete OutdatedInputs], 1UL))
    |> ignore

[<Test>]
let ``If a paused transient method depends on a final transient method, its execution forces reproduction of the upstream method``() = 
    let g = graph { // Fo -> *Pi -> F
        let! a = node0 "a"
        let! b = node1 "b" a
        return! node1 "c" b
    }

    state (g, ["a", Final_MissingOutputOnly; "b", Paused_MissingInputOnly; "c", Final], 0UL)
    |> start "b"
    |> equals (state (g, ["a", Reproduces 1UL; "b", Incomplete TransientInputs; "c", Incomplete OutdatedInputs], 1UL))
    |> succeeded "a" 1UL
    |> equals (state (g, ["a", Final; "b", CanStart 2UL; "c", Incomplete OutdatedInputs], 2UL))
    |> ignore

[<Test>]
let ``Paused transient method cannot be resumed but re-executes and invalidates downstream methods``() = 
    let g = graph { // P -> *Po -> F
        let! a = node0 "a"
        let! b = node1 "b" a
        return! node1 "c" b
    }

    state (g, ["a", Paused; "b", Paused_MissingOutputOnly; "c", Final], 0UL)
    |> start "b"
    |> equals (state (g, ["a", Paused; "b", Started 1UL; "c", Incomplete OutdatedInputs], 1UL))
    |> ignore


[<Test>]
let ``If a final transient method depends on a paused transient method, it cannot be reproduced but re-executes and invalidates downstream methods``() = 
    let g = graph { // Po -> Fio -> *Fio -> Fi
        let! a = node0 "a"
        let! b = node1 "b" a
        let! c = node1 "c" b
        return! node1 "d" c
    }

    state (g, ["a", Paused_MissingOutputOnly; "b", Final_MissingInputOutput; "c", Final_MissingInputOutput; "d", Final_MissingInputOnly], 0UL)
    |> start "c"
    |> equals (state (g, ["a", Started 1UL; "b", Incomplete OutdatedInputs; "c", Incomplete OutdatedInputs; "d", Incomplete OutdatedInputs], 1UL))
    |> ignore