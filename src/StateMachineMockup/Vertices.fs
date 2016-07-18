module Vertices

type TimeIndex = uint64
/// Each of the elements of this array corresponds to the method output with same index
/// and if the output item is a 1d-array, it keeps number of elements in that array; 
/// otherwise, it is zero.
type OutputShape = int list

type IncompleteReason = 
    | UnassignedInputs
    | OutdatedInputs
    | ExecutionFailed   of failure: System.Exception
    | Stopped
    | TransientInputs

type VertexStatus =
    | Final of OutputShape
    | Final_MissingInputOnly of OutputShape    
    | Final_MissingOutputOnly of OutputShape
    | Final_MissingInputOutput of OutputShape
    | Paused of OutputShape 
    | Paused_MissingOutputOnly of OutputShape
    | Paused_MissingInputOnly of OutputShape
    | Paused_MissingInputOutput of OutputShape
    | CanStart of time: TimeIndex 
    | Started of startTime: TimeIndex  
    | Continues of startTime: TimeIndex * OutputShape
    | Reproduces of startTime: TimeIndex * OutputShape
    | ReproduceRequested of OutputShape  
    | Incomplete of reason: IncompleteReason

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

type VertexState =
    { Status : VertexStatus }
    override x.ToString() = sprintf "%A" x.Status

    static member Outdated = { Status = Incomplete OutdatedInputs }
    static member Unassigned = { Status = Incomplete UnassignedInputs }
    static member Final shape = { Status = Final shape }

