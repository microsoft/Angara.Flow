namespace Angara.Data

[<Sealed;Class>]
type MdMap<'k, 'v when 'k : comparison> =
    member IsScalar: bool
    member AsScalar: unit -> 'v    
    static member Empty : MdMap<'k,'v>

[<RequireQualifiedAccess>]
[<NoEquality; NoComparison>]
type MdMapTree<'key, 'value when 'key : comparison> = 
    | Value of 'value
    | Map of Map<'key, MdMapTree<'key, 'value>>

[<RequireQualifiedAccessAttribute>]
module MdMap =
    val empty<'k, 'v when 'k : comparison> : MdMap<'k,'v>
    /// Returns a new md-map which is scalar having the given value for an empty key.
    val scalar: 'v -> MdMap<_,'v>
    val set: 'k list -> MdMap<'k,'v> -> MdMap<'k,'v> -> MdMap<'k,'v>    
    /// Returns a new md-map built from the given `mdmap` by
    /// leaving the elements whose keys start with the `mdkey` and cut off the start of their keys.
    /// Returns `None` if there are no such keys in the mdmap.
    /// For an empty key returns the `mdmap` intact.
    val tryGet : mdkey:'k list -> mdmap:MdMap<'k,'v> -> MdMap<'k,'v> option
    /// Returns a new md-map built from the given `mdmap` by
    /// leaving the elements whose keys start with the `mdkey` and cut off the start of their keys.
    /// Fails if there are no such keys in the mdmap.
    /// For an empty key returns the `mdmap` intact.
    val get : mdkey:'k list -> mdmap:MdMap<'k,'v> -> MdMap<'k,'v>    
    /// Returns a new md-map which contains all elements of the `mdmap`
    /// whose keys start with the `mdkey`.
    /// Returns `MdMap.empty`, if there are no such keys in the mdmap.
    /// For an empty key returns the `mdmap` intact.
    val startingWith : mdkey:'k list -> mdmap:MdMap<'k,'v> -> MdMap<'k,'v>
    /// Returns a sequence of keys and mdmaps contained in the given mdmap, if it is not scalar.
    /// If the given MdMap is scalar or empty, returns an empty sequence.
    val toShallowSeq : MdMap<'k,'v> -> ('k*MdMap<'k,'v>) seq
    /// Views the collection as an enumerable sequence of scalars
    /// contained in the given mdmap with their keys.
    /// The sequence will be ordered by the key at each dimension.
    val toSeq : MdMap<'k,'v> -> ('k list * 'v) seq
    val map: ('v -> 'w) -> MdMap<'k,'v> -> MdMap<'k,'w>
    val mapi: ('k list -> 'v -> 'w) -> MdMap<'k,'v> -> MdMap<'k,'w>
    /// Creates new md-map by replacing items with index of length greater than the `rank` by
    /// trimming the index to contain `rank` elements and the value returned by the given function.
    /// The `replace` function always gets a key of length `rank` and the corresponding md-map.
    val trim : rank:int -> replace:('k list -> MdMap<'k,'v> -> MdMap<'k,'v>) -> MdMap<'k,'v> -> MdMap<'k,'v>
    val merge : ('k list * 'v option * 'v option -> 'v) -> MdMap<'k,'v> -> MdMap<'k,'v> -> MdMap<'k,'v>
    /// Builds a jagged array with element type `'value` and elements corresponding to the md-map values,
    /// using integer keys as array indices.
    /// If the md-map has rank zero, the function fails.
    /// If there are missing elements in the md-map, the function fails.
    val toJaggedArray : MdMap<int,_> -> 't

    val find : mdkey:'k list -> mdmap:MdMap<'k,'v> -> 'v
    val tryFind : mdkey:'k list -> mdmap:MdMap<'k,'v> -> 'v option
    /// Returns a new md-map having the given scalar value for the given md-key.
    val add : 'k list -> 'v -> MdMap<'k,'v> -> MdMap<'k,'v>
    val equal : ('key list -> 'value -> 'value -> bool) -> MdMap<'key,'value> -> MdMap<'key,'value> -> bool
    val toTree : MdMap<'k,'v> -> MdMapTree<'k,'v>