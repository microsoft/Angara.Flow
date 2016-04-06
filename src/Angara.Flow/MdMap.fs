namespace Angare.Data

type MdKey<'key when 'key : comparison> = 'key list

[<RequireQualifiedAccess>]
type MdMap<'key, 'value when 'key : comparison> = 
    | Value of 'value
    | Map of Map<'key, MdMap<'key, 'value>>

module A =
    let mapToArray (getElement: MdMap<int,'a> -> 'b) (map: Map<int,MdMap<int,'a>>) : 'b[] =
            let elts = map |> Map.map (fun _ -> getElement)
            if elts.Count = 0 then Array.empty 
            else 
                let n = 1 + (map |> Map.toList |> List.last |> fst)
                let arr = Array.init n (fun i ->
                    match elts.TryFind i with
                    | Some elt -> elt
                    | None -> failwith "Map has missing elements and cannot be converted to an array")
                arr

[<RequireQualifiedAccess>]
module MdMap =
    let ofItem (value:'value) : MdMap<_,'value> = 
        MdMap.Value value

    let ofPair (key:MdKey<'key>, value:MdMap<'key, 'value>) : MdMap<'key, 'value> =
        Seq.foldBack(fun k sub -> Map.empty |> Map.add k sub |> MdMap.Map) key value

    let rec tryFind (key:MdKey<'key>) (map:MdMap<'key,'value>) : MdMap<'key,'value> option =
        match map, key with
        | _, [] -> Some map
        | MdMap.Map values, k :: tail -> Map.tryFind k values |> Option.bind (tryFind tail)
        | MdMap.Value _, _ :: _ -> None
                                 
    let rec add (key:MdKey<'key>) (value:MdMap<'key,'value>) (map:MdMap<'key,'value>) : MdMap<'key,'value> =
        match map, key with
        | _, [] -> value
        | MdMap.Map values, k :: tail ->             
            let newVal =
                match values |> Map.tryFind k with
                | Some oldVal -> oldVal |> add tail value
                | None -> ofPair (tail, value)
            values |> Map.add k newVal |> MdMap.Map
        | MdMap.Value _, k :: tail ->
            Map.empty |> Map.add k (ofPair (tail, value)) |> MdMap.Map
            
    let tryCount (key:MdKey<'key>) (map:MdMap<'key,'value>) : int option =
        match map |> tryFind key with
        | Some (MdMap.Map values) -> Some values.Count
        | Some (MdMap.Value _) | None -> None

    let isValue = function
        | MdMap.Value _ -> true
        | MdMap.Map _ -> false

    let value = function
        | MdMap.Value v -> v
        | MdMap.Map _ -> failwith "MdMap is expected to be a value but it is a map"

    let toSeq (map:MdMap<'key,'value>) : (MdKey<'key> * 'value) seq =
        let rec f rkey = function
            | MdMap.Value value -> seq{ yield List.rev rkey, value }
            | MdMap.Map values -> 
                values
                |> Map.toSeq
                |> Seq.map(fun (k, subMap) -> f (k :: rkey) subMap)
                |> Seq.concat
        f [] map

    let rec toJaggedArray (map:MdMap<int,'value>) : System.Array =
        match map with
        | MdMap.Value _ -> failwith "A map is expected but a value is found"
        | MdMap.Map subMap ->
            match subMap |> Map.forall(fun _ -> isValue) with
            | true -> // final level
                upcast(subMap |> A.mapToArray (fun t -> match t with MdMap.Value v -> v | MdMap.Map _ -> failwith "Unreachable case"))
            | false ->
                upcast(subMap |> A.mapToArray (fun t -> match t with MdMap.Map _ -> toJaggedArray t | MdMap.Value _ -> failwith "Data is incomplete and has missing elements"))

    let rec map (f:'valueA -> 'valueB) (mdmap:MdMap<'key,'valueA>) : MdMap<'key,'valueB> =
        match mdmap with
        | MdMap.Value v -> MdMap.Value (f v)
        | MdMap.Map values -> MdMap.Map (values |> Map.map(fun _ -> map f))

    let rec mapi (f:MdKey<'key> -> 'valueA -> 'valueB) (mdmap:MdMap<'key,'valueA>) : MdMap<'key,'valueB> =
        let rec g rkey = function
            | MdMap.Value v -> MdMap.Value (f (List.rev rkey) v)
            | MdMap.Map values -> MdMap.Map (values |> Map.map(fun i -> g (i :: rkey)))
        g [] mdmap

    let trim (rank:int) (replace: MdKey<'key> -> MdMap<'key,'value>) (map:MdMap<'key,'value>) : MdMap<'key,'value> =
        let rec f h rkey map =
            if h < 0 then invalidArg "rank" "Rank is negative"
            else if h = 0 then replace (List.rev rkey)
            else
                match map with
                | MdMap.Value _ -> map
                | MdMap.Map values -> MdMap.Map(values |> Map.map (fun k -> f (h-1) (k :: rkey)))
        f rank [] map

    let merge (f:MdKey<'key> * 'value option * 'value option -> 'value) (map1:MdMap<'key,'value>) (map2:MdMap<'key,'value>) : MdMap<'key,'value> =
        let rec g rkey u v =
            match u,v with
            | Some(MdMap.Value a), Some(MdMap.Value b) -> MdMap.Value (f(List.rev rkey, Some a, Some b))
            | Some(MdMap.Value a), None -> MdMap.Value (f(List.rev rkey, Some a, None))
            | None, Some(MdMap.Value b) -> MdMap.Value (f(List.rev rkey, None, Some b))
            | Some(MdMap.Map sub1), Some(MdMap.Map sub2) ->
                let s1 = sub1 |> Map.toSeq |> Seq.map fst |> Set.ofSeq
                let s2 = sub2 |> Map.toSeq |> Seq.map fst |> Set.ofSeq
                let indices = Set.union s1 s2
                let sub = indices |> Set.toSeq |> Seq.map(fun k -> k, g (k :: rkey) (sub1 |> Map.tryFind k) (sub2 |> Map.tryFind k)) |> Map.ofSeq
                MdMap.Map sub
            | Some(MdMap.Map _ as m), _ (* None or Value *) -> m
            | _ (* None or Value *), Some(MdMap.Map _ as m) -> m
            | None, None -> failwith "Unexpected case"
        g [] (Some map1) (Some map2)
