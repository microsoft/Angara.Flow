module ``Flow expression tests``

open NUnit.Framework

open System
open Angara
open Angara.MethodDecl
open Angara.Execution

let inc = decl (fun x -> x + 1) "inc integer" |> arg "x" |> result1 "out"
let incf = decl (fun x -> x + 1.0) "inc float" |> arg "x" |> result1 "out"
let add = decl (fun x y -> x + y) "add values" |> arg "x" |> arg "y" |> result1 "sum"
let sum = decl (fun (vals: int[]) -> Array.fold (+) 0 vals) "summarize values" |> arg "values" |> result1 "sum"

let minimizeFunc = 
    decl (fun (f: float->float) ((a,b):float*float) (eps:float) ->
        System.Diagnostics.Trace.WriteLine(sprintf "Number of threads: %d" System.Environment.ProcessorCount)
        let ctx = Angara.RuntimeContext.getContext()
        let rec minseq (a,b) eps k = seq{
                System.Threading.Thread.Sleep(5)
                if ctx.Token.IsCancellationRequested then
                    System.Diagnostics.Trace.WriteLine("minf cancelled")
                else
                    let xm = (a + b) * 0.5
                    yield xm
                    if(b-a >= eps) then
                        let x1, x2 = let d = (b - a)*0.25 in xm - d, xm + d
                        let (a,b) = match f x1, f x2 with
                                        | (fx1, fx2) when fx1 > fx2 -> (x1, b)
                                        | (fx1, fx2)                -> (a, x2)
                        yield! minseq (a,b) eps (k+1) 
            }
        minseq (a,b) eps 0) "minf" 
    |> arg "a" |> arg "b" |> arg "eps"
    |> iter |> result1 "out"


/// Input "rank": a positive number.
/// Output "array" is int array with values [0,1,...,rank-1].
let range = 
    decl (fun (rank) -> if rank > 0 then Array.init rank (fun i -> i) else failwith "Rank must be positive") "range"
    |> arg "rank" |> result1 "array" 


[<Test; Category("CI")>]
[<Timeout(2000)>]
let ``Flow expression returns unit``() =
    let f = flow {
        return ()
    }         
    let s = build f
    Assert.AreEqual(0, s.Graph.Structure.Vertices.Count, "Number of methods")
    Assert.AreEqual(0, s.Vertices.Count, "Number of vertex states")

    let f = flow {
        let x = (value 2)
        return ()
    }         
    let s = build f
    Assert.AreEqual(0, s.Graph.Structure.Vertices.Count, "Number of methods")

    let f = flow {
        let! x = makeValue 2
        return ()
    }         
    let s = build f
    Assert.AreEqual(1, s.Graph.Structure.Vertices.Count, "Number of methods")


[<Test; Category("CI")>]
[<Timeout(2000)>]
let ``Increment an integer number``() =
    let f = flow {
        let! x = makeValue 3
        let! y = inc x
        return y
    }         
    
    let s = build f
    Assert.AreEqual(2, s.Graph.Structure.Vertices.Count, "Number of methods")

    let y = run1 f
    Assert.AreEqual(4, y, "Incremented value")


[<Test; Category("CI")>]
[<Timeout(3000)>]
let ``Iterative method with multiple outputs``() =
    let fiter = (decl (fun n ->
        let rec r(k) = seq { 
            yield k
            if k < n then yield! r(k+1)
        }
        r 0) "iter") |> arg "n" |> iter |> result1 "out"
    
    let f = flow {
        let! r = fiter(value 3)
        return r
    }

    Assert.AreEqual(3, run1 f)

 
[<Test; Category("CI")>]
[<Timeout(3000)>]
let ``Array of flows to array of artefacts``() =
    let f = flow {
        let! a = [|
            for i = 1 to 3 do yield flow {
                let! x = inc (value i)
                return x
            }
        |]
        let! s = sum (collect a)
        return s
    }

    Assert.AreEqual(9, run1 f)



[<Test; Category("CI")>]
[<Timeout(2000)>]
let ``A single constant computation expression``() =
    let g = flow {
        let pi = value 3.14
        return pi, pi, (value 2.71828)
    }
     
    Assert.AreEqual((3.14, 3.14, 2.71828), run3 g)
    
    let s = build g
    Assert.AreEqual(2, s.Graph.Structure.Vertices.Count, "Number of methods")

    
[<Test; Category("CI")>]
[<Timeout(2000)>]
let ``Inline constants in "return"``() =
    let g = flow {
        return! add (value 1) (value 2)
    } 

    Assert.AreEqual(3, run1 g)
        
    let s = build g
    Assert.AreEqual(3, s.Graph.Structure.Vertices.Count, "Number of methods")


[<Test; Category("CI")>]
[<Timeout(2000)>]
let ``Inline constants in computation expressions``() =
    let g = flow {
        let! x = inc (value 3)

        let two = (value 2)
        let! y = add x two
        let! z = sum (collect [| y; (value 1); two; two; (value 3) |])

        let! w = sum (value [| 5; 6; 7 |])
        return y,z,w
    } 

        
    Assert.AreEqual((6, 14, 18), run3 g)
        
    let s = build g
    Assert.AreEqual(9, s.Graph.Structure.Vertices.Count, "Number of methods")

[<Test; Category("CI")>]
[<Timeout(2000)>]
let ``Build and run1 a flow using 'flow' expression``() =
    let g = flow {
        let! x = makeValue 3
        let! y = inc x
        let! z = add x y
        return z
    } 
        
    Assert.AreEqual(7, run1 g)

[<Test; Category("CI")>]
[<Timeout(2000)>]
let ``Get an exception occured during execution``() =
    let fail = decl (fun _ -> raise (new System.DivideByZeroException())) "throw exception" |> arg "x" |> result1 "out"
    let g = flow {
        let! x = makeValue 3
        let! y = fail x
        return y
    } 

    try    
        let _ = run1 g
        Assert.Fail "Flow must fail"
    with 
        | :? Control.FlowFailedException as e when e.InnerExceptions.Count = 1 ->
            Assert.IsInstanceOf<System.DivideByZeroException>(e.InnerExceptions.[0])


[<Test; Category("CI")>]
[<Timeout(2000)>]
let ``Return two artefacts from 'flow' monad``() =
    let g = flow {
        let! x = makeValue 3
        let! y = inc x
        let! z = add x y
        return y, z
    }
    let y,z = run2 g
    Assert.AreEqual(4, y, "inc1 value")
    Assert.AreEqual(7, z, "final sum value")

[<Test; Category("CI")>]
[<Timeout(2000)>]
let ``Array input when using 'flow' monad``() =
    let g = flow {
        let! x = makeValue 3
        let! y = inc x
        let! z = inc y            
        let! a = sum (collect [| x; y; z |])
            
        let! w = makeValue [| 1; 2; 7; 10 |]
        let! b = sum w

        return a, b
    }
    let a,b = run2 g
    Assert.AreEqual(12, a, "final sum value")
    Assert.AreEqual(20, b, "final sum value")



[<Test; Category("CI")>]
[<Timeout(2000)>]
let ``Use iterative method in a 'work' monad``() =
    let g = flow {
        let! func   = makeValue (fun (x:float) -> x*x*x + x*x - x)
        let! range  = makeValue (-1.0,1.0)
        let! eps    = makeValue(0.001)
        let! min    = minimizeFunc func range eps
        return! incf min
    } 

    Assert.AreEqual(4.0/3.0, run1 g, 0.001, "Value must be about 4/3")

[<Test; Category("CI")>]
[<Timeout(3000)>]
let ``Vector operation in a work expression``() =
    let w = flow {
        let! s = foreach(value [| 0; 1; 2 |], inc)
        return! sum s
    }

    Assert.AreEqual(6, run1 w, "Sum")


[<Test; Category("CI")>]
[<Timeout(3000)>]
let ``Nested vector operation in a work expression``() =
    let w = flow {
        let! r = makeValue [| 0; 1; 2 |]
        let! s = foreach(r, fun v -> flow {
                let! w = inc v
                let! r2 = range w
                let! s2 = foreach(r2, inc)
                return! sum s2
            })
        return! sum s
    }

    Assert.AreEqual(10, run1 w, "Sum")
    
[<Test; Category("CI")>]
[<Timeout(3000)>]
let ``Closure in a scatter body``() =
    let w = flow {
        let! n = makeValue 5
        let! s = foreach(value [| 0; 1; 2 |], fun v -> flow{
            return! add v n
        })
        return! sum s
    }

    Assert.AreEqual(18, run1 w, "Sum")


[<Test; Category("CI")>]
[<Timeout(3000)>]
let ``Scatter collected array``() =
    let w = flow {
        let! x = makeValue 1
        let! y = makeValue 2
        let! s = foreach(collect [| x; y |], inc)
        return! sum s
    }

    Assert.AreEqual(5, run1 w, "Sum")


[<Test; Category("CI")>]
[<Timeout(3000)>]
let ``Closure in a nested scatter body``() =
    let w = flow {
        let! n = makeValue 5
        let! s = foreach(value [| 0; 1; 2 |], fun v -> flow {
            let! u = inc v
            let! r2 = range u
            let! s2 = foreach(r2, fun w -> flow {
                let! a = add w n // <== "n"
                return! add a v  // <== "v"
            })
            let! z = sum s2
            return! add z n // <== "n"
        })
        return! sum s
    }

    Assert.AreEqual(57, run1 w, "Sum")


[<Test; Category("CI")>]
[<Timeout(3000)>]
let ``Double scatter of 2d-array``() =
    let w = flow {
        let! a = makeValue[| [| 0; 1 |]; [| 2; 3 |] |]

        let! s = foreach(a, fun b -> flow {
            let! r = foreach(b, fun c -> flow {
                return! inc c
            })
            return! sum r
        })
        return! sum s
    }

    Assert.AreEqual(10, run1 w, "Sum")

[<Test; Category("CI")>]
[<Timeout(3000)>]
let ``Triple scatter of 3d-array``() =
    let w = flow {
        let! a = makeValue[| [| [| 0; 1 |]; [| 2; 3 |] |]; [| [| 4; 5 |] |] |]

        let! s = foreach(a, fun b -> flow {
            let! r = foreach(b, fun c -> flow {
                let! t = foreach(c, fun d -> flow {
                    return! inc d
                })
                return! sum t
            })
            return! sum r
        })
        return! sum s
    }

    Assert.AreEqual(21, run1 w, "Sum")


[<Test; Category("CI")>]
[<Timeout(3000)>]
let ``Collecting scattered value``() =
    let w = flow {
        let! s = foreach(value[| 0; 1; 2 |], fun v -> flow {
            let! s = sum(collect[|v; value 1|])
            return s
        })
        return! sum s
    }

    Assert.AreEqual(6, run1 w, "Sum")

[<Test; Category("CI")>]
[<Timeout(3000)>]
let ``Returning scattered value``() =
    let w = flow {
        let! s = foreach(value[| 0; 1; 2 |], fun v -> flow {
            return v
        })
        return! sum s
    }

    Assert.AreEqual(3, run1 w, "Sum")

[<Test; Category("CI")>]
[<Timeout(3000)>]
let ``Scattering collected scattered value``() =
    let w = flow {
        let! s = foreach(value[| 0; 1; 2 |], fun v -> flow {
            let! s = foreach(collect[| value 1; v |], fun w -> flow {
                return! inc w
            })
            return! sum s
        })
        return! sum s
    }

    Assert.AreEqual(12, run1 w, "Sum")

[<Test; Category("CI")>]
[<Timeout(3000)>]
let ``Scatter body doesn't depend on scattered artefact``() =
    let w = flow {
        let! n = makeValue 5
        let! s = foreach(value[|0;1;2|], fun _ -> flow {
            return! inc n
        })
        return! sum s
    }
    
    Assert.AreEqual(18, run1 w, "Sum")

[<Test; Category("CI")>]
[<Timeout(3000)>]
let ``Scatter body doesn't depend on scattered artefact and has no inputs``() =
    let w = flow {
        let! s = foreach(value[|0;1;2|], fun _ -> flow {
            return! makeValue 3
        })
        return s
    }
    
    Assert.AreEqual([|3;3;3|], run1 w, "Sum")

[<Test; Category("CI")>]
[<Timeout(3000)>]
let ``Closure in a nested scatter body using 'collect'``() =
    let w = flow {
        let! n = makeValue 5
        let! r = makeValue [| 0; 1; 2 |]
        let! s = foreach(r, fun v -> flow {
            let! u = inc v
            let! r2 = range u
            let! s2 = foreach(r2, fun w -> flow {
                return! sum (collect[|  w; v; n |])  // <== "v", "n"
            })
            let! z = sum s2
            return! add z n // <== "n"
        })
        return! sum s
    }

    Assert.AreEqual(57, run1 w, "Sum")
    
[<Test; Category("CI")>]
[<Timeout(3000)>]
let ``Closure in a scatter body, delayed closed value producer``() =
    let mkv_delay = decl (fun (delay:int) -> System.Threading.Thread.Sleep(delay); 5) "mkv_delay" |> arg "value" |> result1 "out"

    let w = flow {
        let! n = mkv_delay (value 500)
        let! r = makeValue [| 0; 1; 2 |]
        let! s = foreach(r, fun v -> flow {
            return! add v n
        })
        return! sum s
    }

    Assert.AreEqual(18, run1 w, "Sum")

[<Test; Category("CI")>]
[<Timeout(3000)>]
let ``Closure in a scatter body, delayed array producer``() =
    let mkv_delay = decl(fun (delay:int) -> System.Threading.Thread.Sleep(delay); [| 0; 1; 2 |]) "mkv_delay" |> arg "value" |> result1 "out"

    let w = flow {
        let! n = makeValue 5
        let! r = mkv_delay (value 500)
        let! s = foreach(r, fun v -> flow {
            return! add v n
        })
        return! sum s
    }

    Assert.AreEqual(18, run1 w, "Sum")

[<Test; Category("CI")>]
[<Timeout(3000)>]
let ``Adding two arrays using foreach2``() =
    let w = flow {
        let! a = makeValue [| 1; 2; 4 |]
        let! b = makeValue [| 3; 1; 5 |]

        let! s = foreach2(a, b, fun (a,b) -> flow {
            let! sum = add a b
            return sum
        })
        return! sum s
    }

    Assert.AreEqual(16, run1 w, "Sum")

[<Test; Category("CI")>]
[<Timeout(3000)>]
let ``Adding three arrays using foreach3``() =
    let w = flow {
        let! aa = makeValue [| 1; 2; 4 |]
        let! bb = makeValue [| 3; 1; 5 |]
        let! cc = makeValue [| 4; 3; 2 |]

        let! s = foreach3(aa, bb, cc, fun (a,b,c) -> flow {
            let! sum = add a b
            return! add sum c
        })
        return! sum s // [8;6;11]
    }

    Assert.AreEqual(25, run1 w, "Sum")

[<Test; Category("CI")>]
[<Timeout(3000)>]
let ``Adding three collected arrays using foreach3``() =
    let w = flow {
        let! nums = [| for i = 0 to 5 do yield makeValue i |]
        let aa = collect [| nums.[1]; nums.[2]; nums.[4] |]
        let bb = collect [| nums.[3]; nums.[1]; nums.[5] |]
        let cc = collect [| nums.[4]; nums.[3]; nums.[2] |]

        let! s = foreach3(aa, bb, cc, fun (a,b,c) -> flow {
            let! sum = add a b
            return! add sum c
        })
        return! sum s // [8;6;11]
    }

    Assert.AreEqual(25, run1 w, "Sum")

[<Test; Category("CI")>]
[<Timeout(3000)>]
let ``Nested foreach3``() =
    let w = flow {
        let! aa = makeValue [| 1; 2 |]
        let! bb = makeValue [| 3; 1 |]
        let! cc = makeValue [| 4; 3 |]

        let! s = foreach3(aa, bb, cc, fun (a,b,c) -> flow {
            let! s2 = foreach3(aa, bb, cc, fun (a',b',c') -> flow {
                let! suma = add a a'
                let! sumb = add b b'
                let! sumc = add c c'
                return! sum (collect [|suma; sumb; sumc|])
            })
            return! sum s2
        })
        return! sum s 
    }

    Assert.AreEqual(56, run1 w, "Sum")

[<Test; Category("CI")>]
[<Timeout(3000)>]
let ``Nested foreach3 with collections``() =
    let w = flow {
        let! nums = [| for i = 0 to 5 do yield makeValue i |]
        let aa = collect [| nums.[1]; nums.[2] |]
        let bb = collect [| nums.[3]; nums.[1] |]
        let cc = collect [| nums.[4]; nums.[3] |]

        let! s = foreach3(aa, bb, cc, fun (a,b,c) -> flow {
            let! s2 = foreach3(aa, bb, cc, fun (a',b',c') -> flow {
                let! suma = add a a'
                let! sumb = add b b'
                let! sumc = add c c'
                return! sum (collect [|suma; sumb; sumc|])
            })
            return! sum s2
        })
        return! sum s 
    }

    Assert.AreEqual(56, run1 w, "Sum")

[<Test; Category("CI")>]
[<Timeout(3000)>]
let ``Adding two arrays of different length``() =
    let w = flow {
        let! a = makeValue [| 1; 2; 4 |]
        let! b = makeValue [| 3; 1 |]

        let! s = foreach2(a, b, fun (a,b) -> flow {
            let! sum = add a b
            return sum
        })
        return! sum s
    }

    Assert.Inconclusive("Not implemented: Control.run1 must fail if the state is not final and cannot evolve anymore")
    let s = run1 w
    Assert.Fail("work succeeded")

[<Test; Category("CI")>]
[<Timeout(3000)>]
let ``Nested foreach``() =
    let w = flow {
        let! a = makeValue [| 1; 2; 4 |]
        let! b = makeValue [| 3; 1 |]

        let! s = foreach(a, fun a -> flow {
            let! s1 = foreach(b, fun b -> flow {
                let! s2 = add a b
                return s2
            })
            return! sum s1
        })
        return! sum s
    }

    Assert.AreEqual(26, run1 w)
