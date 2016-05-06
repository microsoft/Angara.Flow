(**
*)

open Angara.Work
open Angara.Flow


(** 

Composing
-------------------------

Declare methods: *)

let inc = decl (fun x -> x + 1) "inc" |> arg "x" |> result1 "out"

(** Build the dependency graph: *)

let w = work {
    let! x = value 1
    let! y = inc x
    return! add x y
}


(** 

Evaluating
---------------------------

Run the flow: *)

let flow : Flow<int> = Flow.Start(w)

let state = flow.Changes |> Flow.pickFinal
let z : int = flow.GetResult state // of typed flow



(** 

Persisting
---------------------------

Save snapshot: *)
Angara.Flow.Register [Angara.Reinstate.Serializers]

Angara.Persistence.Save(flow.State, "flow.zip")

(** Reinstate: *)

(** Continue research: *)


(** 

Inspecting
---------------------------

As web site: *)

open Angara.Html
Angara.Flow.Register [Angara.Html.Serializers]

WriteHtml flow.State

(** View dynamically: *)




    