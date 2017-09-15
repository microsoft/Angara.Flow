module Angara.MethodDecl

open System
open Angara.FlowExpression

type LE = System.Linq.Expressions.Expression

/// data necessary to generate a primitive flow expression
type FEData = {factory:Guid->Method; args:UntypedArtefactExpression list} // collected list of arguments from 

/// the final stage of generation of a primitive flow expression given the collected flow expression data 
let private methd {factory=factory; args=arglist} =
    let m = MethodExpression(factory(Guid.NewGuid()), List.rev arglist) 
    define([m], [|for i in 0..List.length m.Method.Contract.Outputs - 1 -> Single{MethodExpr=m; Index=i}|])

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

/// Creates a pair of a contract and a primitive flow expression given the collected ContractData
let private makeAny 
                (CData(compute, typeArgs, methodId, behaviour, rev_inputs, _, factory): ContractData<_,'compute_result,'compute_result,_,_>)
                (outputs: OutputContract list)
                (unwrap_outputs: 'compute_result -> Execution.Artefact[])
                (wrap_outputs: Execution.Artefact[] -> 'compute_result) = 
                    
    let inputs = List.rev rev_inputs

    let contract : MethodContract =  
        {
            Id=methodId
            TypeArgs=typeArgs
            DisplayName=methodId 
            Description=""
            Inputs = inputs
            Outputs = outputs
        }

    let mfunc : Execution.Artefact list * Execution.MethodCheckpoint option -> (Execution.Artefact list * Execution.MethodCheckpoint) seq =
        match behaviour with
        | SimpleBehaviour ->
            let fsharpfunc = compute.GetType()
            let invoke = fsharpfunc.GetMethods() |> Array.find ( fun mi ->
                (mi.ReturnType = typeof<'compute_result>) &&
                    (let pp = mi.GetParameters()
                    (pp.Length = inputs.Length) && 
                    (Seq.forall2 (fun (p:System.Reflection.ParameterInfo) (i:InputContract) -> 
                        p.ParameterType = i.Type) pp inputs))
                )
            let p_inputs = LE.Parameter(typeof<obj[]>)                                        
            let a_inputs = [for i in 0..(List.length inputs - 1) -> LE.Convert(LE.ArrayAccess(p_inputs,LE.Constant(i)),inputs.[i].Type) :> LE]                                        
            let call = LE.Call(LE.Constant(compute),invoke,a_inputs)
            let bf : Func<obj[], 'compute_result> = LE.Lambda<Func<obj[], 'compute_result>>(call, p_inputs).Compile()
            let smplFun = fun (margs,_:Execution.MethodCheckpoint option) -> Seq.singleton (bf.Invoke(margs |> List.toArray) |> unwrap_outputs |> Array.toList, null)
            smplFun


        | IterativeBehaviour ->
            let fsharpfunc = compute.GetType()
            let invoke = fsharpfunc.GetMethods() |> Array.find ( fun mi ->
                (mi.ReturnType = typeof<'compute_result seq>) &&
                    (let pp = mi.GetParameters()
                    (pp.Length = inputs.Length) && 
                    (Seq.forall2 (fun (p:System.Reflection.ParameterInfo) (i:InputContract) -> 
                        p.ParameterType = i.Type) pp inputs))
                )
            let p_inputs = LE.Parameter(typeof<obj[]>)                                        
            let a_inputs = [for i in 0..(List.length inputs - 1) -> LE.Convert(LE.ArrayAccess(p_inputs,LE.Constant(i)),inputs.[i].Type) :> LE]                                        
            let call = LE.Call(LE.Constant(compute),invoke,a_inputs)
            let bf = LE.Lambda<System.Func<obj[], 'compute_result seq>>(call, p_inputs).Compile()
            let iterFun = fun (margs,_:Execution.MethodCheckpoint option) -> bf.Invoke(margs |> List.toArray) |> Seq.map(fun art -> art |> unwrap_outputs |> Array.toList, null)
            iterFun

        //| ResumableBehaviour ->
        //    let fsharpfunc = compute.GetType()
        //    let invoke = fsharpfunc.GetMethods() |> Array.find ( fun mi ->
        //        (mi.ReturnType = typeof<'compute_result seq>) &&
        //            (let pp = mi.GetParameters()
        //            (pp.Length = inputs.Length+1) && 
        //            (Seq.forall2 (fun (p:System.Reflection.ParameterInfo) (i:InputContract) -> 
        //                p.ParameterType = i.Type) (pp |> Seq.skip 1) inputs))
        //        )
        //    let s_input = LE.Parameter(typeof<'compute_result option>)                                        
        //    let p_inputs = LE.Parameter(typeof<obj[]>)                                        
        //    let a_inputs = (s_input:>LE)::[for i in 0..(List.length inputs - 1) -> LE.Convert(LE.ArrayAccess(p_inputs,LE.Constant(i)),inputs.[i].Type) :> LE]                                        
        //    let call = LE.Call(LE.Constant(compute),invoke,a_inputs)
        //    let bf = LE.Lambda<System.Func<'compute_result option, obj[], 'compute_result seq>>(call, s_input, p_inputs).Compile()
        //    Angara.Execution.Behavior.ResumableIterativeFunction(fun (argstate,margs) ->
        //        bf.Invoke(
        //                argstate |> Option.map wrap_outputs,
        //                margs) |> Seq.map unwrap_outputs)
        | CompoundBehaviour 
        | _ ->
            failwith "not implemented"

    factory methd {factory = (fun id -> upcast FunctionMethod(id, contract, mfunc)); args = []}

/// Declare a simple method producing one output with a given 'method id'. The 'method id' is by convention a unique combination of two words.
let result1
    out1
    (data: ContractData<_,'a,'a,FlowExpression<ArtefactExpression<'a>>,_>) =

    makeAny data [
                ({Name=out1; Type=typeof<'a>}:OutputContract)
                ] (fun a -> [|a:>obj|]) (function ([|a|] : Execution.Artefact[]) -> (a:?>'a) | _ -> failwith "Internal runtime error")

/// Create a compound method producing two outputs with a given 'method id'. The 'method id' is by convention a unique combination of two words.
let result2 
    (out1,out2) 
    (data: ContractData<_,'a*'b,'a*'b,FlowExpression<ArtefactExpression<'a>*ArtefactExpression<'b>>,_>) =

    makeAny data [
                ({Name=out1; Type=typeof<'a>}:OutputContract)
                ({Name=out2; Type=typeof<'b>}:OutputContract)
                ] (fun (a,b) -> [|a:>obj; b:>obj|]) (function ([|a; b|] : Execution.Artefact[]) -> (a:?>'a,b:?>'b) | _ -> failwith "Internal runtime error")


/// Create a compound method producing three outputs with a given 'method id'. The 'method id' is by convention a unique combination of two words.
let result3 
    (out1,out2,out3) 
    (data: ContractData<_,'a*'b*'c,'a*'b*'c,FlowExpression<ArtefactExpression<'a>*ArtefactExpression<'b>*ArtefactExpression<'c>>,_>) =

    makeAny data [
                ({Name=out1; Type=typeof<'a>}:OutputContract)
                ({Name=out2; Type=typeof<'b>}:OutputContract)
                ({Name=out3; Type=typeof<'c>}:OutputContract)
                ] (fun (a,b,c) -> [|a:>obj; b:>obj; c:>obj|]) (function ([|a; b; c|] : Execution.Artefact[]) -> (a:?>'a,b:?>'b,c:?>'c) | _ -> failwith "Internal runtime error")

/// Create a compound method producing three outputs with a given 'method id'. The 'method id' is by convention a unique combination of two words.
let result4 
    (out1,out2,out3,out4) 
    (data: ContractData<_,'a*'b*'c*'d,'a*'b*'c*'d,FlowExpression<ArtefactExpression<'a>*ArtefactExpression<'b>*ArtefactExpression<'c>*ArtefactExpression<'d>>,_>) =

    makeAny data [
                ({Name=out1; Type=typeof<'a>}:OutputContract)
                ({Name=out2; Type=typeof<'b>}:OutputContract)
                ({Name=out3; Type=typeof<'c>}:OutputContract)
                ({Name=out4; Type=typeof<'d>}:OutputContract)
                ] (fun (a,b,c,d) -> [|a:>obj; b:>obj; c:>obj; d:>obj|]) (function ([|a; b; c; d|] : Execution.Artefact[]) -> (a:?>'a,b:?>'b,c:?>'c,d:?>'d) | _ -> failwith "Internal runtime error")


/// Begin new method declaration.
/// Creates the ContractData value that contains the method implementation function and the method id,
/// but no information about the method arguments.
let decl (compute:'c) methodId : ContractData<'c,'c,'statearg,'generator,'generator> =
    CData(compute, [], methodId, SimpleBehaviour, [], [], fun x -> x)
    // in a full declaration x:'generator will be inferred as, e.g. ArtefactExpression<'a> -> ArtefactExpression<b> -> FlowDef<ArtefactExpression<c>>

/// Begin new method declaration.
/// Creates the ContractData value that contains the method implementation function and the method id,
/// but no information about the method arguments.
let gdecl (compute:'c) (typeArgs: System.Type list) methodId : ContractData<'c,'c,'statearg,'generator,'generator> =
    CData(compute, typeArgs, methodId, SimpleBehaviour, [], [], fun x -> x)


/// Add information about a method argument to ContractData value.
let arg 
    (name:string) 
    (CData(compute, typeArgs, methodId, behaviour, inputs, lambdas, factory):ContractData<'compute,'arg->'reduced,'statearg, ArtefactExpression<'arg>->'rest, 'generator>) 
    : ContractData<'compute,'reduced,'statearg,'rest,'generator> =

    let input:InputContract = {Name=name; Type=typeof<'arg>}
    let arg_we : (FEData -> 'rest) -> FEData -> ArtefactExpression<'arg> -> 'rest = fun f da arg ->
            f {da with args=arg.Untyped::da.args}
    CData(compute, typeArgs, methodId, behaviour, input::inputs, lambdas, arg_we >> factory)

/// Declares a method as iterative. Must immediately precede a call to 'make'.
let iter (CData(compute, typeArgs, methodId, behaviour, inputs, lambdas, factory):ContractData<'compute,'reduced seq,'statearg, 'rest, 'generator>) 
    : ContractData<'compute,'reduced,'statearg,'rest,'generator> =
    let behaviour' = match behaviour with SimpleBehaviour -> IterativeBehaviour | _ -> behaviour
    CData(compute, typeArgs, methodId, behaviour', inputs, lambdas, factory)

/// Decalres a method as resumable.
/// The first argument of the method implementation must be an option of the method result type.
/// This function must precede all calls to 'arg'.
let statearg 
    (CData(compute, typeArgs, methodId, behaviour, inputs, lambdas, factory):ContractData<'ret option->'reduced,'ret option->'reduced, 'ret, 'rest, 'generator>) 
    : ContractData<'ret option->'reduced,'reduced,'ret,'rest,'generator> =
    CData(compute, typeArgs, methodId, ResumableBehaviour, inputs, lambdas, factory)

