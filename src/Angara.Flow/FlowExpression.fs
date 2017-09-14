namespace Angara

[<AutoOpen>]
module FlowExpression =

    open Angara.Graph
    open Microsoft.FSharp.Reflection
    open System
    
    type Contract = 
        { Inputs: Type list
        ; Outputs: Type list}

    /// Method representation in a flow definition such that 
    /// (i) it keeps incoming dependencies;
    /// (ii) it allows to instantiate a method.
    type MethodExpression(m: Method, inputs: UntypedArtefactExpression list) =
        member x.Id : Guid = m.Id

        /// Returns an ordered list of other method's outputs that are connected as inputs to this method.
        member x.Inputs : UntypedArtefactExpression list = 
            inputs

        member x.Method : Method = m
        

    and /// A method output.
        [<StructuredFormatDisplay("{%A}")>] 
        Output = 
        { MethodExpr: MethodExpression; Index: OutputRef }    
        member private x.``%A`` = x.ToString()
        override x.ToString() = "<<code commented>>"
            //sprintf "(%s from %s %s)" 
            //    x.Method.Contract.Outputs.[x.Index].Name
            //    x.Method.Contract.Description.DisplayName 
            //    (match x.Method.Suffix with Auto s -> "?"+s | UserDefined s -> s)

    and /// An untyped reference to an artefact.
        UntypedArtefactExpression =
        /// Artefact is an output of a method.
        | Single        of Output
        /// Artefact is a result of "collect" function, i.e. represents a virtual array-artefact collected from a sequence of artefacts.
        | Collection    of Output list
        /// Represents a typed artefact of a work expression that can be evaluated into a value of certain type.
        override x.ToString() =
            match x with
            | Single output -> output.ToString()
            | Collection outputs -> sprintf "%A" outputs

    and /// A typed reference to an artefact, where generic type parameter 'a is used just to keep the artefact type.
        ArtefactExpression<'a> = 
            A of UntypedArtefactExpression
                override x.ToString() = match x with A a -> a.ToString()
                member x.Untyped = match x with A a -> a

    and Scope =
        { Origin    :   MethodExpression list
        ; Scopes    :   Scope list }

    
    /// Composition of Angara methods that can be used to produce to a flow dependencies graph, and an evaluation target which represents artefacts to be computed.
    /// The `result parameter is a type of Results artefacts, e.g. Artefact<int> * Artefact<double>.
    and FlowExpression<'result> =
        { Methods   :   MethodExpression list
        ; Results   :   UntypedArtefactExpression[]
        ; Scopes    :   Scope list } // List of 'id' methods that represents foreach bodies variables.


    let define<'result>(methods: MethodExpression list, results: UntypedArtefactExpression[]) : FlowExpression<'result> = 
        { Methods = methods
        ; Results = results
        ; Scopes  = List.empty }


    type FlowExpressionBuilder() =
        let inpm = function | Single output -> [output.MethodExpr] 
                            | Collection outputs-> outputs |> List.map(fun o -> o.MethodExpr)
        let inpm_list(a:UntypedArtefactExpression list) = a |> Seq.map(inpm) |> Seq.distinct |> Seq.concat |> Seq.toList

        
        /// Some input artefacts of the "methods" can be "inline", i.e. are not bound using "let!" but created in-place using "let".
        /// Here we find and return methods for such artefacts to be added to the monad as well.
        let inlineMethods methods =
            methods
            |> Seq.collect (fun (m:MethodExpression) -> 
                            let isInline output = not (methods |> List.contains output.MethodExpr)
                            m.Inputs |> Seq.collect (function | Single output -> if output |> isInline then Seq.singleton output else Seq.empty
                                                              | Collection outputs-> outputs |> Seq.filter isInline))
            |> Seq.map (fun (o:Output) -> o.MethodExpr) |> Seq.toList

        let isArtefactType (t:System.Type) =
            t.IsGenericType && t.GetGenericTypeDefinition() = typedefof<ArtefactExpression<_>>

        /// Make a complete flow output tuple (0..*) of typed artefacts from an array of untyped flow result artefacts.
        /// E.g. if 'a is typeof<Artefact<int> * Artefact<double>> then flow's result is a tuple (int*double),
        /// and this method returns a tuple containing the two artefact instances as a tuple Artefact<int> * Artefact<double>.
        /// Reason to have this function is that FlowDef<'a> keeps results as untyped artefacts and that cannot be changed,
        /// because otherwise we wouldn't be able to declare such type in general and still operate with individual result instance.
        let makeOutput (f:FlowExpression<'a>) : 'a (* : Artefact<int> * Artefact<double>, for example *) =
        
            /// make a typed artefact from an untype artefact plus its type information using reflection
            let makeArtefact (t:System.Type) (s:UntypedArtefactExpression) =
                assert (isArtefactType t)
                t.GetMethod("NewA").Invoke(null,[|s|])

            if FSharpType.IsTuple typeof<'a> then
                let types = FSharpType.GetTupleElements typeof<'a>
                assert (Array.length types = Array.length f.Results)
                FSharpValue.MakeTuple(Array.map2 makeArtefact types f.Results, typeof<'a>) :?> 'a
            elif typeof<'a> = typeof<unit> then
                () :> obj :?> 'a
            else
                assert (1 = Array.length f.Results)
                assert (typedefof<'a> = typedefof<ArtefactExpression<_>>)
                (makeArtefact typeof<'a> f.Results.[0]) :?> 'a

        /// Make FlowDef out of typed result artefacts tuple.
        /// E.g. results : Artefact<int> * Artefact<double>
        let flowFromResults (results:'a) : FlowExpression<'a> =
            let asUntypedArtefact (a:obj (* : Artefact<_>*)) : UntypedArtefactExpression =
                let t = a.GetType()
                assert(isArtefactType t)
                t.GetProperty("Item").GetMethod.Invoke(a, [||]) :?> UntypedArtefactExpression

            let inpm = function | Single output -> [output.MethodExpr] 
                                | Collection outputs-> outputs |> List.map(fun o -> o.MethodExpr)
            let untypedResults =
                if FSharpType.IsTuple typeof<'a> then
                    FSharpValue.GetTupleFields results |> Array.map asUntypedArtefact
                elif typeof<'a>=typeof<unit> then [||]
                else
                    [|asUntypedArtefact results|]
            let methods = untypedResults |> Seq.map(inpm) |> Seq.distinct |> Seq.concat |> Seq.toList
            define(methods, untypedResults)

        
        member x.Bind(expr:FlowExpression<'T>, follow:'T -> FlowExpression<'U>) : FlowExpression<'U> =        
            let inlineMethods = inlineMethods expr.Methods
            let followWrk = follow (makeOutput expr)
            let methods = (Seq.append inlineMethods (Seq.append expr.Methods followWrk.Methods)) |> Seq.distinct |> Seq.toList
            { define(methods, followWrk.Results) with Scopes = expr.Scopes @ followWrk.Scopes }
        
        member x.Bind(expr:FlowExpression<'T>[], follow:'T[] -> FlowExpression<'U>) : FlowExpression<'U> =        
            let extendWithInlines (w:FlowExpression<'T>) :FlowExpression<'T> =
                let methods' = Seq.append (inlineMethods w.Methods) w.Methods |> Seq.distinct |> Seq.toList
                { define(methods', w.Results) with Scopes = w.Scopes }

            let expr_w_inlines = Array.map extendWithInlines expr
            let followWrk = follow (Array.map makeOutput expr_w_inlines)
            let all_methods = expr_w_inlines |> Seq.map (fun w -> w.Methods) |> Seq.concat |> Seq.append followWrk.Methods |> Seq.distinct |> Seq.toList
            let exprScopes = expr_w_inlines |> Seq.map (fun w -> w.Scopes) |> Seq.concat |> Seq.toList
            { define(all_methods, followWrk.Results) with Scopes = exprScopes @ followWrk.Scopes }


        member x.Return(target:ArtefactExpression<'a>*ArtefactExpression<'b>*ArtefactExpression<'c>*ArtefactExpression<'d>*ArtefactExpression<'e>*ArtefactExpression<'f>*ArtefactExpression<'g>) : FlowExpression<ArtefactExpression<'a>*ArtefactExpression<'b>*ArtefactExpression<'c>*ArtefactExpression<'d>*ArtefactExpression<'e>*ArtefactExpression<'f>*ArtefactExpression<'g>> = 
            flowFromResults(target)

        member x.Return(target:ArtefactExpression<'a>*ArtefactExpression<'b>*ArtefactExpression<'c>*ArtefactExpression<'d>*ArtefactExpression<'e>*ArtefactExpression<'f>) : FlowExpression<ArtefactExpression<'a>*ArtefactExpression<'b>*ArtefactExpression<'c>*ArtefactExpression<'d>*ArtefactExpression<'e>*ArtefactExpression<'f>> = 
            flowFromResults(target)

        member x.Return(target:ArtefactExpression<'a>*ArtefactExpression<'b>*ArtefactExpression<'c>*ArtefactExpression<'d>*ArtefactExpression<'e>) : FlowExpression<ArtefactExpression<'a>*ArtefactExpression<'b>*ArtefactExpression<'c>*ArtefactExpression<'d>*ArtefactExpression<'e>> = 
            flowFromResults(target)

        member x.Return(target:ArtefactExpression<'a>*ArtefactExpression<'b>*ArtefactExpression<'c>*ArtefactExpression<'d>) : FlowExpression<ArtefactExpression<'a>*ArtefactExpression<'b>*ArtefactExpression<'c>*ArtefactExpression<'d>> = 
            flowFromResults(target)

        member x.Return(target:ArtefactExpression<'a>*ArtefactExpression<'b>*ArtefactExpression<'c>) : FlowExpression<ArtefactExpression<'a>*ArtefactExpression<'b>*ArtefactExpression<'c>> = 
            flowFromResults(target)

        member x.Return(target:ArtefactExpression<'a>*ArtefactExpression<'b>) : FlowExpression<ArtefactExpression<'a>*ArtefactExpression<'b>> = 
            flowFromResults(target)

        member x.Return(target:ArtefactExpression<'a>) : FlowExpression<ArtefactExpression<'a>> = 
            flowFromResults(target)

        member x.Return(_:unit) = define([], [||]) : FlowExpression<unit>

        member x.Return() = define([], [||]) : FlowExpression<unit>

        member x.ReturnFrom(expr:FlowExpression<'a>) : FlowExpression<'a> =
            let inlineMethods = inlineMethods expr.Methods
            let methods = Seq.append inlineMethods expr.Methods |> Seq.distinct |> Seq.toList
            { define(methods, expr.Results) with Scopes = expr.Scopes }


        member x.Zero() : FlowExpression<unit> = define(List.empty<MethodExpression>, [||])

    /// Builds a strongly typed dependency graph from Angara methods using computation expression syntax.
    let flow = FlowExpressionBuilder()

   
    open Angara.States
    open Angara.Execution


    /// Takes any value and generates a work expression that evaluates to this value.
    let makeValue<'a> (v:'a) : FlowExpression<ArtefactExpression<'a>> =  
        let me = MethodExpression(Contracts.createMakeValue v, [])
        define([me], [|Single({ MethodExpr = me; Index = 0 })|])

    /// Converts any value into an artefact without binding it to an identifier.
    let value (v:'a) : ArtefactExpression<'a>  = 
        let me = MethodExpression(Contracts.createMakeValue v, [])
        let a:ArtefactExpression<'a> = A(Single({ MethodExpr = me; Index = 0}))
        a

    module FrameworkContracts =
        let id_ContractName = "id_"

        let isId (c:MethodContract) = c.Id.StartsWith(id_ContractName)

    type internal build_x_state = 
        { 
            IdToMethod: Map<MethodId, Method>
            State: State<Method, MethodOutput> 
        } with 
        static member Empty = 
            { 
                IdToMethod = Map.empty
                State = 
                {
                    Graph = FlowGraph.empty
                    Vertices = Map.empty
                    TimeIndex = 0UL
                }
            }

    //let internal isOptimizable (v:Method) wmeta = 
    //    FrameworkContracts.isId v.Contract &&
    //    match Annotations.GetMethodMetadata v wmeta with
    //    | Some meta -> isAutogenerated meta
    //    | None -> false

    
    /// Removes automatically added id methods (represented by 'scopes') if they are not needed
    /// Rules:
    ///    scatter -> id -> o2o     ==> scatter, OR
    ///    scatter -> id -> reduce  ==> o2o,     OR
    ///    o2o     -> id -> o2o     ==> o2o              (e.g. foreach is applied to result of another foreach)
    ///    reduce  -> id -> o2o     ==> reduce
    ///    reduce  -> id -> scatter ==> o2o
    ///
    /// Remarks: scatter -> id -> collect, scatter -> id -> scatter cannot be replaced with single edge
    //let rec internal build_x_optimize_ids (work : build_x_state) (ids: Method list): build_x_state =
    //    let rec tryOptimize_id work v_id (inEdge : Edge<Method>) outEdgeCriteria : build_x_state  =          
    //        let removeId work (v_id:MethodVertex) = 
    //            { id_map = work.id_map.Remove v_id.Id
    //            ; graph = work.graph.TryRemove v_id |> Option.get
    //            ; state = work.state.Remove v_id
    //            ; names = work.names.Remove v_id
    //            ; meta = work.meta |> Annotations.RemoveMethod v_id }

    //        let outEdges = work.graph.Structure.OutEdges v_id |> Seq.toList
    //        let batch = outEdges |> List.choose(fun outEdge ->
    //                if outEdgeCriteria outEdge then
    //                    let repeatIds = seq {
    //                                        if isOptimizable inEdge.Source work.meta then yield inEdge.Source
    //                                        if isOptimizable outEdge.Target work.meta then yield outEdge.Target
    //                                    } |> Seq.toList
    //                    // connecting 'in' and 'out'-s directly
    //                    Some((outEdge.Target, outEdge.InputRef, outEdge.Source, outEdge.OutputRef), // disconnect
    //                         (outEdge.Target, outEdge.InputRef, inEdge.Source, inEdge.OutputRef), // connect
    //                         repeatIds)  
    //                else None)
    //        if batch.Length > 0 then 
    //            let work' = { work with graph = work.graph.BatchAlter (batch |> List.map (fun (dc,_,_) -> dc)) (batch |> List.map (fun (_,cn,_) -> cn)) }
    //            let rep = batch |> Seq.map(fun (_,_,rep) -> rep) |> Seq.concat |> Seq.distinct |> Seq.toList
    //            let work'' = if outEdges.Length = batch.Length then removeId work' v_id else work'                    
    //            build_x_optimize_ids work'' rep // allows to optimize chains of ids
    //        else if outEdges.Length = 0 then
    //            let work' = removeId work v_id 
    //            if isOptimizable inEdge.Source work.meta then build_x_optimize_ids work' [inEdge.Source] else work'
    //        else work 

    //    let build_x_optimize_id (work : build_x_state) (v_id: Method) =
    //        let graph = work.State.Graph          
    //        match graph.Structure.Vertices.Contains v_id with
    //        | true -> // vertex still exists 
    //            let inEdges = graph.Structure.InEdges v_id |> Seq.toArray
    //            match inEdges.Length, inEdges.[0].Type with 
    //            | 1, Scatter  _ -> tryOptimize_id work v_id inEdges.[0] (fun outEdge -> match outEdge.Type with OneToOne _ | Reduce _ -> true | Scatter _ | Collect _             -> false)
    //            | 1, OneToOne _ -> tryOptimize_id work v_id inEdges.[0] (fun outEdge -> match outEdge.Type with OneToOne _            -> true | Reduce _  | Scatter _ | Collect _ -> false)
    //            | 1, Reduce   _ -> tryOptimize_id work v_id inEdges.[0] (fun outEdge -> match outEdge.Type with OneToOne _ | Scatter _-> true | Reduce _  | Collect _             -> false)
    //            | _ -> work // do not change anything            
    //        | false -> work // already optimized
    //    ids |> List.fold build_x_optimize_id work     


    /// Build flow dependency graph from the given flow definition.
    let build<'result> (def:FlowExpression<'result>) : State<Method, MethodOutput> =
        let methods = def.Methods       

        /// adds a method and all its inputs to a graph and returns a modified graph
        let rec add_method (work: build_x_state) (m: MethodExpression) : build_x_state =
            if work.IdToMethod |> Map.containsKey m.Id then work
            else
                // The method isn't in the graph. Create a vertex and add it to the graph.
                let v_target = m.Method
                let state = { work.State with Graph = work.State.Graph.Add v_target }
                let work_w_target = 
                    { IdToMethod = work.IdToMethod |> Map.add v_target.Id v_target
                    ; State = state }

                // Add all inputs in order
                m.Inputs 
                |> Seq.mapi (fun idx input -> (idx, input))
                |> Seq.fold (fun wrk (idx, input:UntypedArtefactExpression) ->
                    let connect output = // connect whole source output to the target port 'idx'
                        let m_source = output.MethodExpr
                        let wrk_w_source = add_method wrk m_source
                        let v_source = wrk_w_source.IdToMethod.[m_source.Id]
                        { wrk_w_source with State = { wrk_w_source.State with Graph = wrk_w_source.State.Graph.Connect (v_target, idx) (v_source, output.Index) } }
                    let connect_collected w output = // connect collected source output to the target port 'idx'
                        let m_source = output.MethodExpr
                        let w_w_source = add_method w m_source
                        let v_source = w_w_source.IdToMethod.[m_source.Id]
                        { w_w_source with State = { w_w_source.State with Graph = w_w_source.State.Graph.ConnectItem (v_target, idx) (v_source, output.Index) } }
                    match input with
                    | Single(output) -> connect output
                    | Collection(outputs) -> outputs |> List.fold connect_collected wrk
                    ) work_w_target 

        // Builds work graph from methods
        let work1 = methods |> List.fold add_method build_x_state.Empty

        // Graph optimization:
        // - Removes automatically added id methods if they are not needed
        //let ids = work1.State.Graph.Structure.Vertices |> Set.filter(fun v -> isOptimizable v work1.meta) |> Set.toList
        //let work2 = build_x_optimize_ids work1 ids
        let work2 = work1

        work2.State


    /// Returns an array of vertices and their outputs which are evaluation targets for the work expression.
    /// Automatically added "id" methods could be optimized but still remain as evaluation target.
    /// In that case we go upstream by chain of "id" to find the actual evaluation target vertex.
    let findResultMethods (f:FlowExpression<_>) (wg:FlowGraph<Method>) : (Method*OutputRef)[] = 
        let rec find (a:UntypedArtefactExpression) = 
            match a with
                | Single output ->                    
                    match wg.Structure.Vertices |> Seq.tryFind (fun v -> v.Id = output.MethodExpr.Id) with
                    | Some v -> v, output.Index
                    | None ->
                        if FrameworkContracts.isId output.MethodExpr.Method.Contract then find output.MethodExpr.Inputs.[0]
                        else failwith "Cannot find a vertex representing a work evaluation target"
                | Collection _ -> failwith "Cannot evaluate a collection of artefacts"
        f.Results |> Array.map find

    let run (def:FlowExpression<ArtefactExpression<'result>>) : 'result = 
        assert(1 = def.Results.Length)

        let initial = build def
        let final = Control.runToFinal initial

        let m = findResultMethods def final.Graph 
        final |> Control.outputScalar m.[0] 