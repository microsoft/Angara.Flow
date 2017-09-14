module ``Flow expression tests``

open NUnit.Framework

open System
open System.Threading
open System.Reactive.Linq
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

