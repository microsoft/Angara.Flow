namespace Angara

[<AutoOpen>]
module FlowExpression =
    open Angara.States
    open Angara.Execution

    type UntypedArtefactExpression
    type ArtefactExpression<'a>

    type FlowExpression<'result>

    [<Sealed>]
    type FlowExpressionBuilder =
        new : unit -> FlowExpressionBuilder

        member Bind : FlowExpression<'T> * ('T -> FlowExpression<'U>) -> FlowExpression<'U>
        member Bind : FlowExpression<'T>[] * ('T[] -> FlowExpression<'U>) -> FlowExpression<'U>

//        member Return : (ArtefactExpression<'a>*ArtefactExpression<'b>*ArtefactExpression<'c>*ArtefactExpression<'d>*ArtefactExpression<'e>*ArtefactExpression<'f>*ArtefactExpression<'g>) -> FlowExpression<ArtefactExpression<'a>*ArtefactExpression<'b>*ArtefactExpression<'c>*ArtefactExpression<'d>*ArtefactExpression<'e>*ArtefactExpression<'f>*ArtefactExpression<'g>>  
//        member Return : (ArtefactExpression<'a>*ArtefactExpression<'b>*ArtefactExpression<'c>*ArtefactExpression<'d>*ArtefactExpression<'e>*ArtefactExpression<'f>) -> FlowExpression<ArtefactExpression<'a>*ArtefactExpression<'b>*ArtefactExpression<'c>*ArtefactExpression<'d>*ArtefactExpression<'e>*ArtefactExpression<'f>> 
//        member Return : (ArtefactExpression<'a>*ArtefactExpression<'b>*ArtefactExpression<'c>*ArtefactExpression<'d>*ArtefactExpression<'e>) ->  FlowExpression<ArtefactExpression<'a>*ArtefactExpression<'b>*ArtefactExpression<'c>*ArtefactExpression<'d>*ArtefactExpression<'e>> 
//        member Return : (ArtefactExpression<'a>*ArtefactExpression<'b>*ArtefactExpression<'c>*ArtefactExpression<'d>) ->  FlowExpression<ArtefactExpression<'a>*ArtefactExpression<'b>*ArtefactExpression<'c>*ArtefactExpression<'d>> 
//        member Return : (ArtefactExpression<'a>*ArtefactExpression<'b>*ArtefactExpression<'c>) ->  FlowExpression<ArtefactExpression<'a>*ArtefactExpression<'b>*ArtefactExpression<'c>> 
//        member Return : (ArtefactExpression<'a>*ArtefactExpression<'b>) -> FlowExpression<ArtefactExpression<'a>*ArtefactExpression<'b>> 
//        member Return : (ArtefactExpression<'a>) -> FlowExpression<ArtefactExpression<'a>> 
        member Return : unit -> FlowExpression<unit>

        member ReturnFrom : FlowExpression<'a> -> FlowExpression<'a> 
        member Zero : unit -> FlowExpression<unit> 


    val flow : FlowExpressionBuilder

    /// Takes any value and generates a work expression that evaluates to this value.
    val makeValue<'a> : 'a -> FlowExpression<ArtefactExpression<'a>> 

    /// Converts any value into an artefact without binding it to an identifier.
    val value : 'a -> ArtefactExpression<'a>

    /// Collects zero or more individual artefacts into an array-artefact.
    val collect<'a> : ArtefactExpression<'a>[] -> ArtefactExpression<'a[]>

    /// Creates a work which evaluates the given body function for each item of the given vector.
    val foreach : ArtefactExpression<'T[]> * (ArtefactExpression<'T> -> FlowExpression<ArtefactExpression<'U>>) -> FlowExpression<ArtefactExpression<'U[]>>

    /// Creates a work which evaluates the given body function for each item of the given vector.
    val foreach2 : ArtefactExpression<'T[]> * ArtefactExpression<'S[]> * (ArtefactExpression<'T> * ArtefactExpression<'S> -> FlowExpression<ArtefactExpression<'U>>) -> FlowExpression<ArtefactExpression<'U[]>>

    /// Creates a work which evaluates the given body function for each item of the given vector.
    val foreach3 : ArtefactExpression<'T[]> * ArtefactExpression<'S[]> * ArtefactExpression<'R[]> * (ArtefactExpression<'T> * ArtefactExpression<'S> * ArtefactExpression<'R> -> FlowExpression<ArtefactExpression<'U>>) -> FlowExpression<ArtefactExpression<'U[]>>

    /// Build flow dependency graph from the given flow definition.
    val build<'result> : FlowExpression<'result> -> State<Method, MethodOutput>

    /// Runs the flow and waits untils the results are computed.
    val run4 : (FlowExpression<ArtefactExpression<'a>*ArtefactExpression<'b>*ArtefactExpression<'c>*ArtefactExpression<'d>>) -> 'a*'b*'c*'d 
    val run3 : (FlowExpression<ArtefactExpression<'a>*ArtefactExpression<'b>*ArtefactExpression<'c>>) -> 'a*'b*'c 
    val run2 : (FlowExpression<ArtefactExpression<'a>*ArtefactExpression<'b>>) -> 'a*'b 
    val run1 : (FlowExpression<ArtefactExpression<'a>>) -> 'a


module MethodDecl =
    open System
    open FlowExpression

    type FEData = {factory:Guid->Method; args:UntypedArtefactExpression list} // collected list of arguments from 
    type BehaviourTag = SimpleBehaviour | IterativeBehaviour | ResumableBehaviour | CompoundBehaviour

    /// The collected data for creating a contract with make* functions
    type ContractData<'compute,     // a type of the original function to be wrapped, e.g. 'a -> 'b -> 'c
                      'reduced,     // intermediate type of the original function stripped out of arguments, e.g. 'b -> 'c
                      'statearg,    // type-check for the first argument of a resumable function
                      'flow,        // intermediate type of a corresponding generator stripped out of arguments, e.g. ArtefactExpression<'b> -> FlowDef<ArtefactExpression<'c>>
                      'generator    // the inferred type of the corresponding generator, e.g. ArtefactExpression<'a> -> ArtefactExpression<'b> -> FlowDef<ArtefactExpression<'c>>
                      > = 
        CData of
            'compute // the F# function to be wrapped
             * Type list // List of type arguments for this instance of contract
             * string // contract id
             * BehaviourTag
             * InputContract list  // inputs
             * Execution.MethodId list       // lambdas
             * ((FEData -> 'flow) -> FEData -> 'generator) // factory of the primitive flow expression generator
    

    /// Begin new method declaration.
    /// Creates the ContractData value that contains the method implementation function and the method id,
    /// but no information about the method arguments.
    val decl : 'compute -> string -> ContractData<'compute,'compute,'statearg,'generator,'generator> 

    /// Begin new method declaration.
    /// Creates the ContractData value that contains the method implementation function and the method id,
    /// but no information about the method arguments.
    val gdecl : 'compute -> Type list -> string -> ContractData<'compute,'compute,'statearg,'generator,'generator> 

    /// Add information about a method argument to ContractData value.
    val arg :
        string ->
        ContractData<'compute,'arg->'reduced,'statearg, ArtefactExpression<'arg>->'rest, 'generator> ->
        ContractData<'compute,'reduced,'statearg,'rest,'generator>

    /// Declares a method as iterative. Must immediately precede a call to 'result'.
    val iter : ContractData<'compute,'reduced seq,'statearg, 'rest, 'generator> -> ContractData<'compute,'reduced,'statearg,'rest,'generator>

    /// Create a compound method producing four outputs with a given 'method id'. The 'method id' is by convention a unique combination of two words.
    val result4 :
        (string*string*string*string) ->
        (ContractData<'compute,'a*'b*'c*'d,'a*'b*'c*'d,FlowExpression<ArtefactExpression<'a>*ArtefactExpression<'b>*ArtefactExpression<'c>*ArtefactExpression<'d>>,'r>) ->
        'r
    /// Create a compound method producing three outputs with a given 'method id'. The 'method id' is by convention a unique combination of two words.
    val result3 :
        (string*string*string) ->
        (ContractData<'compute,'a*'b*'c,'a*'b*'c,FlowExpression<ArtefactExpression<'a>*ArtefactExpression<'b>*ArtefactExpression<'c>>,'r>) ->
        'r
    /// Create a compound method producing two outputs with a given 'method id'. The 'method id' is by convention a unique combination of two words.
    val result2 :
        (string*string) ->
        (ContractData<'compute,'a*'b,'a*'b,FlowExpression<ArtefactExpression<'a>*ArtefactExpression<'b>>,'r>) ->
        'r
    /// Create a compound method producing one output with a given 'method id'. The 'method id' is by convention a unique combination of two words.
    val result1 :
        (string) ->
        (ContractData<'compute,'a,'a,FlowExpression<ArtefactExpression<'a>>,'r>) ->
        'r

