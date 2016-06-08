module Vertices

type TimeIndex = uint64

type IncompleteReason = 
    | UnassignedInputs
    | OutdatedInputs
    | ExecutionFailed   of failure: System.Exception
    | Stopped
    | TransientInputs

type VertexStatus =
    | Final              
    | Final_MissingInputOnly    
    | Final_MissingOutputOnly
    | Final_MissingInputOutput
    | Paused 
    | Paused_MissingOutputOnly
    | Paused_MissingInputOnly
    | Paused_MissingInputOutput
    | CanStart of time: TimeIndex 
    | Started of startTime: TimeIndex  
    | Continues of startTime: TimeIndex  
    | Reproduces of startTime: TimeIndex
    | ReproduceRequested   
    | Incomplete of reason: IncompleteReason

    member x.IsUpToDate = match x with Final | Reproduces _ | ReproduceRequested _ | Paused | Continues _ -> true | _ -> false
    member x.IsIncompleteReason reason = match x with Incomplete r when r = reason -> true | _ -> false

type VertexState =
    { Status : VertexStatus }
    override x.ToString() = sprintf "%A" x.Status


