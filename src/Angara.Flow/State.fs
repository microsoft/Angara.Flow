namespace Angara.States

open Angara
open Angara.Graph

type TimeIndex = uint64

type IncompleteReason = 
    | UnassignedInputs
    | OutdatedInputs
    | ExecutionFailed   of failure: System.Exception
    | Stopped
    | TransientInputs
    override this.ToString() =
        match this with
        | UnassignedInputs -> "Method has unassigned inputs"
        | OutdatedInputs -> "Method has outdated inputs"
        | ExecutionFailed failure -> "Execution failed: " + (match failure with null -> "<na>" | _ -> failure.ToString())
        | Stopped -> "Stopped"
        | TransientInputs -> "Transient inputs"

type OutputShape = int list

type VertexStatus =
    | Final of OutputShape       
    | Final_MissingInputOnly of OutputShape    
    | Final_MissingOutputOnly of OutputShape
    | Final_MissingInputOutput of OutputShape                         
    | Paused of OutputShape 
    | Paused_MissingOutputOnly of OutputShape
    | Paused_MissingInputOnly of OutputShape
    | Paused_MissingInputOutput of OutputShape
    | CanStart                  of time: TimeIndex 
    | Started                   of startTime: TimeIndex      
    | Continues                 of startTime: TimeIndex * outputShape : OutputShape      
    | Reproduces                of startTime: TimeIndex * outputShape : OutputShape
    | ReproduceRequested of OutputShape   
    | Incomplete                of reason: IncompleteReason
   
    override x.ToString() = sprintf "%A" x
    member x.IsUpToDate = match x with Final _ | Reproduces _ | ReproduceRequested _ | Paused _ | Continues _ -> true | _ -> false
    member x.IsIncompleteReason reason = match x with Incomplete r when r = reason -> true | _ -> false
    member x.TryGetShape() = 
        match x with
        | Final s
        | Final_MissingInputOnly s    
        | Final_MissingOutputOnly s
        | Final_MissingInputOutput s
        | Paused s 
        | Paused_MissingOutputOnly s
        | Paused_MissingInputOnly s
        | Paused_MissingInputOutput s
        | Continues (_,s)
        | Reproduces (_,s)
        | ReproduceRequested s -> Some s
        | CanStart _
        | Started _
        | Incomplete _ -> None


[<Interface>]
type IVertexData =
    abstract member Shape : OutputShape

type VertexState<'d when 'd:>IVertexData> = 
    { Status : VertexStatus
      Data : 'd option
    } with 
    override x.ToString() = sprintf "Vertex is %A" x.Status
    static member Unassigned : VertexState<'d> = 
        { Status = VertexStatus.Incomplete (IncompleteReason.UnassignedInputs); Data = None }
    static member Outdated : VertexState<'d> = 
        { Status = VertexStatus.Incomplete (OutdatedInputs); Data = None }
    static member Final (data: 'd) =
        { Status = VertexStatus.Final (data.Shape); Data = Some data }
    static member FinalEmpty rank : VertexState<'d> =
        { Status = VertexStatus.Final (List.init rank (fun _ -> 0)); Data = None }

type State<'v,'d when 'v:comparison and 'v:>IVertex and 'd :> IVertexData> =
    { Graph : FlowGraph<'v>
      Vertices : VerticesState<'v, VertexState<'d>> 
      TimeIndex : TimeIndex }

type VertexChanges<'d when 'd:>IVertexData> = 
    | New of MdVertexState<VertexState<'d>>
    | Removed 
    | ShapeChanged of old:MdVertexState<VertexState<'d>> * current:MdVertexState<VertexState<'d>> * isConnectionChanged:bool 
    | Modified of indices:Set<VertexIndex> * old:MdVertexState<VertexState<'d>> * current:MdVertexState<VertexState<'d>> * isConnectionChanged:bool 
    
type Changes<'v,'d when 'v : comparison and 'd :> IVertexData> = Map<'v, VertexChanges<'d>>

type StateUpdate<'v,'d when 'v:comparison and 'v:>IVertex and 'd :> IVertexData> = 
    { State : State<'v,'d>
      Changes : Changes<'v,'d> }

type VertexItem<'v when 'v:comparison and 'v:>IVertex> = 'v * VertexIndex

[<AutoOpen>]
module StateOperations =
    open Angara.Data

    let add (update : StateUpdate<_,_>) v vs = 
        { State = { update.State with Vertices = update.State.Vertices |> Map.add v vs }
          Changes = update.Changes |> Map.add v (VertexChanges.New vs) }

    let update (update : StateUpdate<'v,'d>) (vi : VertexItem<'v>) (vsi : VertexState<'d>) =
        let v, i = vi
        let vs = update.State.Vertices |> Map.find v
        let nvs = vs |> MdMap.add i vsi
        let c = 
            match update.Changes |> Map.tryFind v with
            | Some(New _) -> New(nvs)
            | Some(ShapeChanged(oldvs,_,connChanged)) -> ShapeChanged(oldvs,nvs,connChanged)
            | Some(Modified(indices,oldvs,_,connChanged)) -> Modified(indices |> Set.add i, oldvs, nvs, connChanged)
            | Some(Removed) -> failwith "Removed vertex cannot be modified"
            | None -> Modified(Set.empty |> Set.add i, vs, nvs, false) 

        { State = { update.State with Vertices = update.State.Vertices |> Map.add v nvs }
          Changes = update.Changes.Add(v, c) }

    let replace (update : StateUpdate<'v,'d>) (v : 'v) (vs : MdMap<int,VertexState<'d>>) =
        let c = 
            match update.Changes |> Map.tryFind v with
            | Some(New _) -> New(vs)
            | Some(ShapeChanged(oldvs,_,connChanged)) -> ShapeChanged(oldvs,vs,connChanged)
            | Some(Modified(_,oldvs,_,connChanged)) -> ShapeChanged(oldvs,vs,connChanged)
            | Some(Removed) -> failwith "Removed vertex cannot be modified"
            | None -> ShapeChanged(update.State.Vertices.[v],vs,false)

        { State = { update.State with Vertices = update.State.Vertices |> Map.add v vs }
          Changes = update.Changes.Add(v, c) }

