module internal List

open System

let rec replaceAt (i:int) (v:'a) (l:'a list) =    
    match l with
    | [] -> raise (IndexOutOfRangeException())
    | x::xs when i = 0 -> v::xs
    | x::xs -> x::(replaceAt (i-1) v xs)

let rec removeAt (i:int) (l:'a list) =    
    match l with
    | [] -> raise (IndexOutOfRangeException())
    | x::xs when i = 0 -> xs
    | x::xs -> x::(removeAt (i-1) xs)

let rec remove (v:'a) (l:'a list) =    
    match l with
    | [] -> []
    | x::xs when x = v -> xs 
    | x::xs -> x::(remove v xs)

let rec replace (v: 'a) (w: 'a) (l:'a list) =
    match l with
    | [] -> []
    | x::xs when x = v -> w::xs
    | x::xs -> replace v w xs

let rec addOrReplace (selector: 'a -> bool) (w: 'a) (l:'a list) =
    match l with
    | [] -> [w]
    | x::xs when selector(x) -> w::xs
    | x::xs -> addOrReplace selector w xs

let rec contains (v: 'a) (l: 'a list) =
    match l with
    | [] -> false
    | x::[] -> x = v
    | x::xs -> x = v || contains v xs

let takeEqualOrLess (n: int) (l: 'a list) : 'a list =
    let rec f (n: int) (head: 'a list) (tail: 'a list) : 'a list =
        if n = 0 then head
        else match tail with
                | [] -> head
                | x::[] -> head @ [x]
                | x::xs -> f (n-1) (head @ [x]) xs
    f n [] l

let removeLast (l: 'a list) : 'a list = 
    let rec f (h: 'a list) (t: 'a list) =
        match t with
        | [] -> failwith "List is empty" // possible only on first call
        | x::[] -> h
        | x::xs -> f (h@[x]) xs
    f [] l

let rec last (l: 'a list) : 'a =
    match l with
    | [] -> failwith "List is empty"
    | x::[] -> x
    | x::xs -> last xs