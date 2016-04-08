namespace Angara.Data

[<RequireQualifiedAccess>]
[<NoEquality; NoComparison>]
type internal MdMapTree<'key, 'value when 'key : comparison> = 
    | Value of 'value
    | Map of Map<'key, MdMapTree<'key, 'value>>

[<RequireQualifiedAccess>]
module internal MdMapTree =
    type OptionBuilder() =
        member x.Bind(v,f) = Option.bind f v
        member x.Return v = Some v
        member x.ReturnFrom o = o
    let opt = OptionBuilder()

    let internal ofPair (key:'key list, value:MdMapTree<'key, 'value>) : MdMapTree<'key, 'value> =
        Seq.foldBack(fun k sub -> Map.empty |> Map.add k sub |> MdMapTree.Map) key value
                                 
    let rec set (key:'key list) (value:MdMapTree<'key,'value>) (map:MdMapTree<'key,'value>) : MdMapTree<'key,'value> =
        match map, key with
        | _, [] -> value
        | MdMapTree.Map values, k :: tail ->             
            let newVal =
                match values |> Map.tryFind k with
                | Some oldVal -> oldVal |> set tail value
                | None -> ofPair (tail, value)
            values |> Map.add k newVal |> MdMapTree.Map
        | MdMapTree.Value _, k :: tail ->
            Map.empty |> Map.add k (ofPair (tail, value)) |> MdMapTree.Map

    let rec tryGet (key:'key list) (map:MdMapTree<'key,'value>) : MdMapTree<'key,'value> option =
        match map, key with
        | _, [] -> Some map
        | MdMapTree.Map values, k :: tail -> Map.tryFind k values |> Option.bind (tryGet tail)
        | MdMapTree.Value _, _ :: _ -> None

    let startingWith (key:'key list) (map:MdMapTree<'key,'value>) : MdMapTree<'key,'value> option =
        let rec f key map = 
            match map, key with
            | _, [] -> Some map
            | MdMapTree.Map values, k :: tail -> 
                opt {
                    let! kv = Map.tryFind k values
                    let! newKv = f tail kv
                    return MdMapTree.Map (Map.empty.Add(k, newKv))
                }
            | MdMapTree.Value _, _ :: _ -> None
        f key map

    let toSeq (map:MdMapTree<'key,'value>) : ('key list * 'value) seq =
        let rec f rkey = function
            | MdMapTree.Value value -> seq{ yield List.rev rkey, value }
            | MdMapTree.Map values -> 
                values
                |> Map.toSeq
                |> Seq.map(fun (k, subMap) -> f (k :: rkey) subMap)
                |> Seq.concat
        f [] map

    let internal mapToArray (getElement: MdMapTree<int,'a> -> 'b) (map: Map<int,MdMapTree<int,'a>>) : 'b[] =
            let elts = map |> Map.map (fun _ -> getElement)
            if elts.Count = 0 then Array.empty 
            else 
                let n = 1 + (map |> Map.toList |> List.last |> fst)
                let arr = Array.init n (fun i ->
                    match elts.TryFind i with
                    | Some elt -> elt
                    | None -> failwith "Map has missing elements and cannot be converted to an array")
                arr

    let internal isValue = function
        | MdMapTree.Value _ -> true
        | MdMapTree.Map _ -> false

    let rec toJaggedArray (map:MdMapTree<int,'value>) : System.Array =
        match map with
        | MdMapTree.Value _ -> failwith "A map is expected but a value is found"
        | MdMapTree.Map subMap ->
            match subMap |> Map.forall(fun _ -> isValue) with
            | true -> // final level
                upcast(subMap |> mapToArray (fun t -> match t with MdMapTree.Value v -> v | MdMapTree.Map _ -> failwith "Unreachable case"))
            | false ->
                upcast(subMap |> mapToArray (fun t -> match t with MdMapTree.Map _ -> toJaggedArray t | MdMapTree.Value _ -> failwith "Data is incomplete and has missing elements"))

    let rec map (f:'valueA -> 'valueB) (mdmap:MdMapTree<'key,'valueA>) : MdMapTree<'key,'valueB> =
        match mdmap with
        | MdMapTree.Value v -> MdMapTree.Value (f v)
        | MdMapTree.Map values -> MdMapTree.Map (values |> Map.map(fun _ -> map f))

    let rec mapi (f:'key list -> 'valueA -> 'valueB) (mdmap:MdMapTree<'key,'valueA>) : MdMapTree<'key,'valueB> =
        let rec g rkey = function
            | MdMapTree.Value v -> MdMapTree.Value (f (List.rev rkey) v)
            | MdMapTree.Map values -> MdMapTree.Map (values |> Map.map(fun i -> g (i :: rkey)))
        g [] mdmap

    let trim (rank:int) (replace: 'key list -> MdMapTree<'key,'value>) (map:MdMapTree<'key,'value>) : MdMapTree<'key,'value> =
        let rec f h rkey map =
            if h < 0 then invalidArg "rank" "Rank is negative"
            else if h = 0 then replace (List.rev rkey)
            else
                match map with
                | MdMapTree.Value _ -> map
                | MdMapTree.Map values -> MdMapTree.Map(values |> Map.map (fun k -> f (h-1) (k :: rkey)))
        f rank [] map

    let merge (f:'key list * 'value option * 'value option -> 'value) (map1:MdMapTree<'key,'value>) (map2:MdMapTree<'key,'value>) : MdMapTree<'key,'value> =
        let rec g rkey u v =
            match u,v with
            | Some(MdMapTree.Value a), Some(MdMapTree.Value b) -> MdMapTree.Value (f(List.rev rkey, Some a, Some b))
            | Some(MdMapTree.Value a), None -> MdMapTree.Value (f(List.rev rkey, Some a, None))
            | None, Some(MdMapTree.Value b) -> MdMapTree.Value (f(List.rev rkey, None, Some b))
            | Some(MdMapTree.Map sub1), Some(MdMapTree.Map sub2) ->
                let s1 = sub1 |> Map.toSeq |> Seq.map fst |> Set.ofSeq
                let s2 = sub2 |> Map.toSeq |> Seq.map fst |> Set.ofSeq
                let indices = Set.union s1 s2
                let sub = indices |> Set.toSeq |> Seq.map(fun k -> k, g (k :: rkey) (sub1 |> Map.tryFind k) (sub2 |> Map.tryFind k)) |> Map.ofSeq
                MdMapTree.Map sub
            | Some(MdMapTree.Map _ as m), _ (* None or Value *) -> m
            | _ (* None or Value *), Some(MdMapTree.Map _ as m) -> m
            | None, None -> failwith "Unexpected case"
        g [] (Some map1) (Some map2)


[<Sealed;Class>]
type MdMap<'key, 'value when 'key : comparison> internal (tree : MdMapTree<'key, 'value>) =
    static let empty = MdMap(MdMapTree<'key,'value>.Map(Map.empty))

    static member Empty = empty

    member x.IsScalar = match tree with MdMapTree.Value _ -> true | MdMapTree.Map _ -> false
    member x.AsScalar() = match tree with MdMapTree.Value v -> v | MdMapTree.Map _ -> invalidOp "The instance is not a scalar"

    member internal x.Tree = tree


[<RequireQualifiedAccess>]
module MdMap =
    let empty<'key, 'value when 'key : comparison> = MdMap<'key,'value>.Empty
    let scalar (value:'value) : MdMap<'key,'value> = MdMap(MdMapTree.Value value)

    let set (key:'key list) (value:MdMap<'key,'value>) (map:MdMap<'key,'value>) : MdMap<'key,'value> =
        MdMap(MdMapTree.set key value.Tree map.Tree)

    let add (key:'key list) (value:'value) (map:MdMap<'key,'value>) : MdMap<'key,'value> =
        MdMap(MdMapTree.set key (MdMapTree.Value value) map.Tree)
    
    let rec tryGet (key:'key list) (map:MdMap<'key,'value>) : MdMap<'key,'value> option =
        MdMapTree.tryGet key map.Tree |> Option.map(fun t -> MdMap(t))

    let rec get (key:'key list) (map:MdMap<'key,'value>) : MdMap<'key,'value> =
        match tryGet key map with
        | Some m -> m
        | None -> raise (new System.Collections.Generic.KeyNotFoundException("The md-map doesn't contain the given key"))

    let startingWith (key:'key list) (map:MdMap<'key,'value>) : MdMap<'key,'value> =
        match MdMapTree.startingWith key map.Tree with
        | Some m -> MdMap(m)
        | None -> empty

    let rec tryFind (key:'key list) (map:MdMap<'key,'value>) : 'value option =
        match MdMapTree.tryGet key map.Tree with
        | Some (MdMapTree.Value v) -> Some v
        | Some (MdMapTree.Map _) | None -> None

    let rec find (key:'key list) (map:MdMap<'key,'value>) : 'value =
        match MdMapTree.tryGet key map.Tree with
        | Some (MdMapTree.Value v) -> v
        | Some (MdMapTree.Map _) -> invalidOp "The given key corresponds to a md-map but not a scalar"
        | None -> raise (new System.Collections.Generic.KeyNotFoundException("The md-map doesn't contain the given key"))

    let toShallowSeq (map:MdMap<'key,'value>) : ('key*MdMap<'key,'value>) seq =
        match map.Tree with
        | MdMapTree.Value _ -> Seq.empty
        | MdMapTree.Map sub -> sub |> Map.toSeq |> Seq.map(fun (k, v)-> k, MdMap(v))

    let toSeq (map:MdMap<'key,'value>) : ('key list * 'value) seq =
        map.Tree |> MdMapTree.toSeq 

    let rec map (f:'valueA -> 'valueB) (map:MdMap<'key,'valueA>) : MdMap<'key,'valueB> =
        MdMap(map.Tree |> MdMapTree.map f)

    let rec mapi (f:'key list -> 'valueA -> 'valueB) (map:MdMap<'key,'valueA>) : MdMap<'key,'valueB> =
        MdMap(map.Tree |> MdMapTree.mapi f)

    let trim (rank:int) (replace: 'key list -> MdMap<'key,'value>) (map:MdMap<'key,'value>) : MdMap<'key,'value> =
        MdMap(map.Tree |> MdMapTree.trim rank (fun k -> (replace k).Tree))

    let merge (f:'key list * 'value option * 'value option -> 'value) (map1:MdMap<'key,'value>) (map2:MdMap<'key,'value>) : MdMap<'key,'value> =
        MdMap(MdMapTree.merge f map1.Tree map2.Tree)

    let toJaggedArray (map:MdMap<int,'value>) : 't =
        let array = MdMapTree.toJaggedArray map.Tree
        array :?> 't
