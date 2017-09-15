module ``Flow expression tests``

open NUnit.Framework

open System
open Angara
open Angara.MethodDecl
open Angara.Execution

let inc = decl (fun x -> x + 1) "inc integer" |> arg "x" |> result1 "out"
let add = decl (fun x y -> x + y) "add values" |> arg "x" |> arg "y" |> result1 "sum"
let sum = decl (fun (vals: int[]) -> Array.fold (+) 0 vals) "summarize values" |> arg "values" |> result1 "sum"

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

    let y = run f
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

    Assert.AreEqual(3, run f)

 
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

    Assert.AreEqual(9, run f)



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

    Assert.AreEqual(3, run g)
        
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
let ``Build and run a flow using 'flow' expression``() =
    let g = flow {
        let! x = makeValue 3
        let! y = inc x
        let! z = add x y
        return z
    } 
        
    Assert.AreEqual(7, run g)

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
        let _ = run g
        Assert.Fail "Flow must fail"
    with 
        | :? Control.FlowFailedException as e when e.InnerExceptions.Count = 1 ->
            Assert.IsInstanceOf<System.DivideByZeroException>(e.InnerExceptions.[0])

