module ``Flow expression tests``

open NUnit.Framework

open System
open Angara
open Angara.MethodDecl

let inc = decl (fun x -> x + 1) "inc integer" |> arg "x" |> result1 "out"

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
    let f = (decl (fun n ->
        let rec r(k) = seq { 
            yield k
            if k < n then yield! r(k+1)
        }
        r 0) "iter") |> arg "n" |> iter |> result1 "out"
    
    let f = flow {
        let! r = f(value 3)
        return r
    }

    Assert.AreEqual(3, run f)