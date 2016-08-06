module StateMachineMockupTests


open NUnit.Framework
open GraphExpression
open StateOperations
open Angara.States


//------------------------------------------------------------------------------------------
//
//    Simple transitions
//
//------------------------------------------------------------------------------------------

[<Test>]
let ``Conducts simple method execution``() = 
    let g = graph {
        return! node1 "a" []
    }

                        state (g, ["a", CanStart 0UL], 0UL)
    |> start "a"
    |>           check (state (g, ["a", Started 1UL], 1UL))
    |> iteration "a" 1UL
    |>           check (state (g, ["a", Continues (1UL,[0])], 2UL))
    |> succeeded "a" 1UL
    |>           check (state (g, ["a", Final [0]], 3UL))
    |> ignore

[<Test>]
let ``Obsolete 'succeeded' message is ignored``() = 
    let g = graph {
        return! node1 "a" []
    }

    // todo: add first start and then restart, so that it would be clear where the obsolete message from
                        state (g, ["a", CanStart 0UL], 0UL)
    |> start "a"
    |>           check (state (g, ["a", Started 1UL], 1UL))
    |> iteration "a" 1UL
    |>           check (state (g, ["a", Continues (1UL,[0])], 2UL))
    |> iteration "a" 0UL // <-- obsolete message
    |>           check (state (g, ["a", Continues (1UL,[0])], 3UL)) // remains executing, without any exceptions
    |> succeeded "a" 0UL // <-- obsolete message
    |>           check (state (g, ["a", Continues (1UL,[0])], 4UL)) // remains executing, without any exceptions
    |> ignore

[<Test>]
let ``Obsolete 'iteration' message is ignored``() = 
    let g = graph {
        return! node1 "a" []
    }

                        state (g, ["a", CanStart 0UL], 0UL)
    |> start "a"
    |>           check (state (g, ["a", Started 1UL], 1UL))
    |> iteration "a" 0UL // <-- obsolete message
    |>           check (state (g, ["a", Started 1UL], 2UL)) // remains started, without any exceptions
    |> ignore

[<Test>]
[<ExpectedException(typeof<System.InvalidOperationException>)>]
let ``Final method cannot be started``() = 
    let g = graph {
        return! node1 "a" []
    }

                        state (g, ["a", Final [0]], 0UL)
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
        let! a = node1 "a" []
        return! node1 "b" [a]
    }

                        state (g, ["a", CanStart 0UL; "b", Incomplete OutdatedInputs], 0UL)
    |> start "a"
    |>           check (state (g, ["a", Started 1UL; "b", Incomplete OutdatedInputs], 1UL))
    |> iteration "a" 1UL
    |>           check (state (g, ["a", Continues (1UL,[0]); "b", CanStart 2UL], 2UL))
    |> succeeded "a" 1UL
    |>           check (state (g, ["a", Final [0]; "b", CanStart 2UL], 3UL))
    |> ignore



[<Test>]
let ``Next method iteration cancels the possible execution of its downstream method``() = 
    let g = graph {
        let! a = node1 "a" []
        return! node1 "b" [a]
    }

                        state (g, ["a", CanStart 0UL; "b", Incomplete OutdatedInputs], 0UL)
    |> start "a"
    |>           check (state (g, ["a", Started 1UL; "b", Incomplete OutdatedInputs], 1UL))
    |> iteration "a" 1UL
    |>           check (state (g, ["a", Continues (1UL,[0]); "b", CanStart 2UL], 2UL))
    |> iteration "a" 1UL                                            
    |>           check (state (g, ["a", Continues (1UL,[0]); "b", CanStart 2UL], 3UL))
    |> start "b"                                                
    |>           check (state (g, ["a", Continues (1UL,[0]); "b", Started 4UL], 4UL))    
    |> iteration "a" 1UL // <-- cancels the execution of "b"; possible obsolete "iteration" from "b" will be ignored.
    |>           check (state (g, ["a", Continues (1UL,[0]); "b", CanStart 5UL], 5UL))
    // todo: iteration "b" 4UL is ignored
    |> succeeded "a" 1UL
    |>           check (state (g, ["a", Final [0]; "b" ,CanStart 5UL], 6UL))
    |> ignore


[<Test>]
let ``Method iteration cancels the execution of the immediate downstream method and makes other downstream methods incomplete``() = 
    let g = graph {
        let! a = node1 "a" []
        let! b = node1 "b" [a]
        return! node1 "c" [b]
    }

    // todo: full scenario from incomplete state

                        state (g, ["a", Continues (0UL,[0]); "b", Continues (0UL,[0]); "c", Continues (0UL,[0])], 0UL)
    |> iteration "a" 0UL
    |>           check (state (g, ["a", Continues (0UL,[0]); "b", CanStart 1UL; "c", Incomplete OutdatedInputs], 1UL))
    |> ignore


[<Test>]
let ``Method iterations can be paused then resumed``() = 
    let g = graph {
        let! a = node1 "a" []
        return! node1 "b" [a]
    }

    // todo: full scenario
                        state (g, ["a", Continues (1UL,[0]); "b", Started 4UL], 4UL)
    |> stop "a"                                            
    |>           check (state (g, ["a", Paused [0]; "b", Started 4UL], 5UL))
    |> iteration "b" 4UL
    |>           check (state (g, ["a", Paused [0]; "b", Continues (4UL,[0])], 6UL))
    |> succeeded "b" 4UL
    |>           check (state (g, ["a", Paused [0]; "b", Final [0]], 7UL))
    |> start "a"
    |>           check (state (g, ["a", Continues (8UL,[0]); "b", Final [0]], 8UL))
    |> iteration "a" 8UL
    |>           check (state (g, ["a", Continues (8UL,[0]); "b", CanStart 9UL], 9UL))
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
        let! a = node1 "a" []
        return! node1 "b" [a]
    }

                        state (g, ["a", Final_MissingOutputOnly [0]; "b", Final_MissingInputOnly [0];], 0UL)
    |> start "a"
    |>           check (state (g, ["a", Reproduces (1UL,[0]); "b", Final_MissingInputOnly [0];], 1UL))
    |> iteration "a" 1UL
    |>           check (state (g, ["a", Reproduces (1UL,[0]); "b", Final_MissingInputOnly [0];], 2UL))
    |> succeeded "a" 0UL // <-- obsolete message (todo: to unit test)
    |>           check (state (g, ["a", Reproduces (1UL,[0]); "b", Final_MissingInputOnly [0];], 3UL))
    |> succeeded "a" 1UL
    |>           check (state (g, ["a", Final [0]; "b", Final [0]], 4UL))
    |> ignore


[<Test>]
let ``If a final transient method depends on another final transient method, its reproduction forces reproduction of the upstream method``() = 
    // Fo -> *Fio
    let g = graph {
        let! a = node1 "a" []
        return! node1 "b" [a]
    }

                        state (g, ["a", Final_MissingOutputOnly [0]; "b", Final_MissingInputOutput [0]], 0UL)
    |> start "b" // <-- reproduction of "b" first requires reproduction of "a" in order to get inputs for "b"
    |>           check (state (g, ["a", Reproduces (1UL,[0]); "b", ReproduceRequested [0]], 1UL))
    |> iteration "a" 1UL
    |>           check (state (g, ["a", Reproduces (1UL,[0]); "b", ReproduceRequested [0]], 2UL))
    |> succeeded "a" 1UL // <-- when "a" is reproduced, "b" can be reproduced as well
    |>           check (state (g, ["a", Final [0]; "b", Reproduces (3UL,[0])], 3UL))
    |> iteration "b" 3UL
    |>           check (state (g, ["a", Final [0]; "b", Reproduces (3UL,[0])], 4UL))
    |> succeeded "b" 3UL
    |>           check (state (g, ["a", Final [0]; "b", Final [0]], 5UL))
    |> ignore

[<Test>]
let ``If a paused transient method depends on another paused transient method, its execution forces re-execution of the upstream method``() = 
    // The transient methods cannot be a result of state machine actions.
    // The only origin of the transiency is deserialization of a state, if some artefacts cannot be serialized/deserialized.
    // The paused method's outputs cannot be reproduced, therefore the "start" makes them to execute and invalidats downstream methods,
    // which were computed for the already missing value.
    
    let g = graph { // Po -> *Pi -> F
        let! a = node1 "a" []
        let! b = node1 "b" [a]
        return! node1 "c" [b]
    }

                        state (g, ["a", Paused_MissingOutputOnly [0]; "b", Paused_MissingInputOnly [0]; "c", Final [0]], 0UL)
    |> start "b"
    |>           check (state (g, ["a", Started 1UL; "b", Incomplete OutdatedInputs; "c", Incomplete OutdatedInputs], 1UL))
    |> ignore

[<Test>]
let ``If a paused transient method depends on a final transient method, its execution forces reproduction of the upstream method``() = 
    let g = graph { // Fo -> *Pi -> F
        let! a = node1 "a" []
        let! b = node1 "b" [a]
        return! node1 "c" [b]
    }

                        state (g, ["a", Final_MissingOutputOnly [0]; "b", Paused_MissingInputOnly [0]; "c", Final [0]], 0UL)
    |> start "b"
    |>           check (state (g, ["a", Reproduces (1UL,[0]); "b", Incomplete TransientInputs; "c", Incomplete OutdatedInputs], 1UL))
    |> succeeded "a" 1UL
    |>           check (state (g, ["a", Final [0]; "b", CanStart 2UL; "c", Incomplete OutdatedInputs], 2UL))
    |> ignore

[<Test>]
let ``Paused transient method cannot be resumed but re-executes and invalidates downstream methods``() = 
    let g = graph { // P -> *Po -> F
        let! a = node1 "a" []
        let! b = node1 "b" [a]
        return! node1 "c" [b]
    }

                        state (g, ["a", Paused [0]; "b", Paused_MissingOutputOnly [0]; "c", Final [0]], 0UL)
    |> start "b"
    |>           check (state (g, ["a", Paused [0]; "b", Started 1UL; "c", Incomplete OutdatedInputs], 1UL))
    |> ignore


[<Test>]
let ``If a final transient method depends on a paused transient method, it cannot be reproduced but re-executes and invalidates downstream methods``() = 
    let g = graph { // Po -> Fio -> *Fio -> Fi
        let! a = node1 "a" []
        let! b = node1 "b" [a]
        let! c = node1 "c" [b]
        return! node1 "d" [c]
    }

                        state (g, ["a", Paused_MissingOutputOnly [0]; "b", Final_MissingInputOutput [0]; "c", Final_MissingInputOutput [0]; "d", Final_MissingInputOnly [0];], 0UL)
    |> start "c"
    |>           check (state (g, ["a", Started 1UL; "b", Incomplete OutdatedInputs; "c", Incomplete OutdatedInputs; "d", Incomplete OutdatedInputs], 1UL))
    |> ignore

[<Test>]
let ``All methods are re-executed if a final transient method depends on two interdepending transient methods and one of them is paused``() = 
    // We check here the case when processing first input implicitly updates the state of the second which then is processed in its turn, but is already outdated.
    // Po -------------> *Fio
    //    ---> Fio ---->
    let g = graph { 
        let! a = node1 "a" []
        let! b = node1 "b" [a]
        return! node1 "c" [a;b]
    }

                        state (g, ["a", Paused_MissingOutputOnly [0]; "b", Final_MissingInputOutput [0]; "c", Final_MissingInputOutput [0]], 0UL)
    |> start "c"
    |>           check (state (g, ["a", Started 1UL; "b", Incomplete OutdatedInputs; "c", Incomplete OutdatedInputs], 1UL))
    |> ignore

    let g = graph { 
        let! a = node1 "a" []
        let! b = node1 "b" [a]
        return! node1 "c" [b;a]
    }

                        state (g, ["a", Paused_MissingOutputOnly [0]; "b", Final_MissingInputOutput [0]; "c", Final_MissingInputOutput [0]], 0UL)
    |> start "c"
    |>           check (state (g, ["a", Started 1UL; "b", Incomplete OutdatedInputs; "c", Incomplete OutdatedInputs], 1UL))
    |> ignore

[<Test>]
let ``All methods are reproduced if a final transient method depends on two interdepending transient final methods``() = 
    // Fo -------------> *Fio
    //    ---> Fio ---->
    let g = graph { 
        let! a = node1 "a" []
        let! b = node1 "b" [a]
        return! node1 "c" [a;b]
    }

                        (*state (g, ["a", Final_MissingOutputOnly [0]; "b", Final_MissingInputOutput [0]; "c", Final_MissingInputOutput [0]], 0UL)
    |> start "c"
    |>           check (state (g, ["a", Reproduces (1UL,[0]); "b", ReproduceRequested [0]; "c", ReproduceRequested [0]], 1UL))
    |> iteration "a" 1UL
    |>           check *)state (g, ["a", Reproduces (1UL,[0]); "b", ReproduceRequested [0]; "c", ReproduceRequested [0]], 2UL) //)
    |> succeeded "a" 1UL
    |>           check (state (g, ["a", Final [0]; "b", Reproduces (3UL,[0]); "c", ReproduceRequested [0]], 3UL))
    |> iteration "b" 3UL
    |>           check (state (g, ["a", Final [0]; "b", Reproduces (3UL,[0]); "c", ReproduceRequested [0]], 4UL))
    |> succeeded "b" 3UL
    |>           check (state (g, ["a", Final [0]; "b", Final [0]; "c", Reproduces (5UL,[0])], 5UL))
    |> ignore

    let g = graph { 
        let! a = node1 "a" []
        let! b = node1 "b" [a]
        return! node1 "c" [b;a]
    }

                        state (g, ["a", Final_MissingOutputOnly [0]; "b", Final_MissingInputOutput [0]; "c", Final_MissingInputOutput [0]], 0UL)
    |> start "c"
    |>           check (state (g, ["a", Reproduces (1UL,[0]); "b", ReproduceRequested [0]; "c", ReproduceRequested [0]], 1UL))
    |> ignore


//------------------------------------------------------------------------------------------
//
//    Vector operations
//
//------------------------------------------------------------------------------------------
open Angara.Data

let scalar = MdMap.scalar

[<Test>]
let ``Scatter: A method with input of type T can be connected to a method producing T[] and it is multiplied for each of the input array elements``() =
    let g = graph { 
        let! a = scatter_node1 "a" []
        return! node1 "b" [a]
    }


                        mdState (g, ["a", scalar (CanStart 0UL); "b", scalar (Incomplete OutdatedInputs)], 0UL)
    |> start "a"
    |>           check (mdState (g, ["a", scalar (Started 1UL); "b", scalar (Incomplete OutdatedInputs)], 1UL))
    |> iteration_array "a" 1UL [2]
    |>           check (mdState (g, ["a", scalar (Continues (1UL,[2])); "b", vector [CanStart 2UL; CanStart 2UL]], 2UL))
    |> mdStart ("b",[0])
    |>           check (mdState (g, ["a", scalar (Continues (1UL,[2])); "b", vector [Started 3UL; CanStart 2UL]], 3UL))
    |> mdStart ("b",[1])
    |>           check (mdState (g, ["a", scalar (Continues (1UL,[2])); "b", vector [Started 3UL; Started 4UL]], 4UL))
    |> iteration_array "a" 1UL [1]
    |>           check (mdState (g, ["a", scalar (Continues (1UL,[1])); "b", vector [CanStart 5UL]], 5UL))
    |> succeeded "a" 1UL
    |>           check (mdState (g, ["a", scalar (Final [1]); "b", vector [CanStart 5UL]], 6UL))
    |> ignore

[<Test>]
let ``Reduce: If a method producing T is multiplied and produces T[], it can be connected to another method accepting T[]``() =
    let g = graph { 
        let! a = scatter_node1 "a" []
        let! b = node1 "b" [a]
        return! reduce_node1 "c" b
    }

                        mdState (g, ["a", scalar (Final [2]); "b", vector [Started 0UL; Started 1UL]; "c", scalar (Incomplete OutdatedInputs)], 1UL)
    |> mdIteration ("b",[0]) 0UL
    |>           check (mdState (g, ["a", scalar (Final [2]); "b", vector [Continues (0UL, [0]); Started 1UL]; "c", scalar (Incomplete OutdatedInputs)], 2UL))
    |> mdIteration ("b",[1]) 1UL
    |>           check (mdState (g, ["a", scalar (Final [2]); "b", vector [Continues (0UL, [0]); Continues (1UL, [0])]; "c", scalar (CanStart 3UL)], 3UL))
    |> start "c" 
    |>           check (mdState (g, ["a", scalar (Final [2]); "b", vector [Continues (0UL, [0]); Continues (1UL, [0])]; "c", scalar (Started 4UL)], 4UL))
    |> mdIteration ("b",[1]) 1UL
    |>           check (mdState (g, ["a", scalar (Final [2]); "b", vector [Continues (0UL, [0]); Continues (1UL, [0])]; "c", scalar (CanStart 5UL)], 5UL))
    |> ignore

[<Test; Category("Scenario")>]
let ``Reduce: if the scattering method produces an empty array, the reducing method should be executed for an empty input array as well``() =
    let g = graph { 
        let! a = scatter_node1 "a" []
        let! b = node1 "b" [a]
        return! reduce_node1 "c" b
    }

                        mdState (g, ["a", scalar (Continues (0UL,[2])); "b", vector [Continues (0UL, [0]); Continues (1UL, [0])]; "c", scalar (Continues (5UL, [0]))], 5UL)
    |> iteration_array "a" 0UL [0]
    |>           check (mdState (g, ["a", scalar (Continues (0UL,[0])); "b", scalar (Final [0]); "c", scalar (CanStart 6UL)], 6UL))
    |> iteration_array "a" 0UL [1]
    |>           check (mdState (g, ["a", scalar (Continues (0UL,[1])); "b", vector [CanStart 7UL]; "c", scalar (Incomplete OutdatedInputs)], 7UL))
    |> ignore


[<Test>]
let ``Reduce: chain of methods depeding on scatter method producing an empty array``() =
    let g = graph { 
        let! a = scatter_node1 "a" []
        let! c = node1 "c" [a]
        let! d = node1 "d" [c]
        let! e = node1 "e" [c;d]
        return! reduce_node1 "f" e
    }

                        mdState (g, ["a", scalar (Continues (0UL,[1])); 
                                     "c", vector [Continues (0UL, [0])]; "d", vector [Continues (0UL, [0])];
                                     "e", vector [Continues (0UL, [0])];
                                     "f", scalar (Continues (0UL,[1])) ], 1UL)
    |> iteration_array "a" 0UL [0]
    |>           check (mdState (g, ["a", scalar (Continues (0UL,[0])); 
                                     "c", scalar (Final [0]); "d", scalar (Final [0]);
                                     "e", scalar (Final [0]);
                                     "f", scalar (CanStart 2UL) ], 2UL))
    |> ignore

[<Test; Category("Scenario")>]
let ``Reduce: vector method has two inputs and one of them produces empty array, another produces non-empty array``() =
    let g = graph { 
        let! a = scatter_node1 "a" []
        let! b = scatter_node1 "b" []
        let! c = node1 "c" [a]
        let! d = node1 "d" [b]
        let! e = node1 "e" [c;d]
        return! reduce_node1 "f" e
    }

                        mdState (g, ["a", scalar (Continues (0UL,[1])); "b", scalar (Continues (0UL,[1])); 
                                     "c", vector [Continues (0UL, [0])]; "d", vector [Continues (0UL, [0])];
                                     "e", vector [Continues (0UL, [0])];
                                     "f", scalar (Continues (0UL,[1])) ], 1UL)
    |> iteration_array "a" 0UL [0]
    |>           check (mdState (g, ["a", scalar (Continues (0UL,[0])); "b", scalar (Continues (0UL,[1])); 
                                     "c", scalar (Final [0]); "d", vector [Continues (0UL, [0])]; 
                                     "e", vector [Incomplete UnassignedInputs]; // "e" has length because it is connected to "d" which still has shape 1.
                                     "f", scalar (Incomplete OutdatedInputs) ], 2UL))
    |> iteration_array "b" 0UL [0]
    |>           check (mdState (g, ["a", scalar (Continues (0UL,[0])); "b", scalar (Continues (0UL,[0])); 
                                     "c", scalar (Final [0]); "d", scalar (Final [0]); 
                                     "e", scalar (Final [0]);
                                     "f", scalar (CanStart 3UL) ], 3UL))
    |> ignore
