namespace Angara.Data

type MdKey<'key when 'key : comparison> = 'key list

[<RequireQualifiedAccess>]
type MdMap<'key, 'value when 'key : comparison> = 
    | Value of 'value
    | Map of Map<'key, MdMap<'key, 'value>>

[<RequireQualifiedAccess>]
module MdMap =
    // Basic    
    val tryFind : MdKey<'key> -> MdMap<'key,'value> -> MdMap<'key,'value> option
    val add : MdKey<'key> -> MdMap<'key,'value> -> MdMap<'key,'value> -> MdMap<'key,'value>
    /// If value of the md-map for the given key is `Map`, returns `Some` of the number of elements of that map;
    /// otherwise, if it is `Value` or there is not value for the given key, returns `None`.
    val tryCount : MdKey<'key> -> MdMap<'key,'value> -> int option
    
    val isValue : MdMap<_,_> -> bool
    val value : MdMap<_,'value> -> 'value

    /// Views the collection as an enumerable sequence of pairs
    /// consisting of key and corresponding item. 
    /// The sequence will be ordered by the keys of the map at each dimension.
    val toSeq : MdMap<'key,'value> -> (MdKey<'key> * 'value) seq

    /// Builds a jagged array with element type `'value` and elements corresponding to the md-map values,
    /// using integer keys as array indices.
    /// If the md-map has rank zero, the function fails.
    /// If there are missing elements in the md-map, the function fails.
    val toJaggedArray: MdMap<int,'value> -> System.Array

    // Utility functions
    val map: ('valueA -> 'valueB) -> MdMap<'key,'valueA> -> MdMap<'key,'valueB>
    val mapi: (MdKey<'key> -> 'valueA -> 'valueB) -> MdMap<'key,'valueA> -> MdMap<'key,'valueB>
    /// Creates new md-map by replacing items with index of length greater than the `rank`.
    val trim : rank:int -> (MdKey<'key> -> MdMap<'key,'value>) -> MdMap<'key,'value> -> MdMap<'key,'value>
    val merge : (MdKey<'key> * 'value option * 'value option -> 'value) -> MdMap<'key,'value> -> MdMap<'key,'value> -> MdMap<'key,'value>
    
    // Construction
    val ofItem: 'value -> MdMap<_,'value>
    val ofPair: MdKey<'key> * MdMap<'key, 'value> -> MdMap<'key, 'value>
