namespace Angara

[<AutoOpen>]
module FlowExpression =

    open Angara.Graph
    open Microsoft.FSharp.Reflection
    open System
    open System.Diagnostics

    /// Method representation in a flow definition such that 
    /// (i) it keeps incoming dependencies;
    /// (ii) it allows to instantiate a method.
    [<DebuggerDisplayAttribute("{Method.Contract.DisplayName} {Id}")>]
    type MethodExpression(m: Method, inputs: UntypedArtefactExpression list) =

        do
            Trace.WriteLine("ME");

        member x.Id : Guid = m.Id

        /// Returns an ordered list of other method's outputs that are connected as inputs to this method.
        member x.Inputs : UntypedArtefactExpression list = 
            inputs

        member x.Method : Method = m
        
        override x.Equals other =
            match other with
            | :? MethodExpression as y -> y.Id = x.Id
            | _ -> false
        override x.GetHashCode() = x.Id.GetHashCode()
        override x.ToString() = 
            sprintf "%s %s%s" 
                m.Contract.DisplayName 
                (m.Contract.Inputs
                    |> List.zip x.Inputs
                    |> List.map (fun (i, c) -> sprintf ", <%s = %s" c.Name (i.ToString()))
                    |> String.concat "") 
                (m.Contract.Outputs
                    |> List.map (fun c -> sprintf ", >%s : %O" c.Name c.Type)
                    |> String.concat "")
        interface System.IComparable with
          member x.CompareTo yobj = 
              match yobj with
              | :? MethodExpression as y -> x.Id.CompareTo(y.Id)
              | _ -> invalidArg "yobj" "cannot compare values of different types"

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
    /// The `result parameter is a type of Results artefacts, e.g. ArtefactExpression<int> * ArtefactExpression<double>.
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
        /// E.g. if 'a is typeof<ArtefactExpression<int> * ArtefactExpression<double>> then flow's result is a tuple (int*double),
        /// and this method returns a tuple containing the two artefact instances as a tuple ArtefactExpression<int> * ArtefactExpression<double>.
        /// Reason to have this function is that FlowDef<'a> keeps results as untyped artefacts and that cannot be changed,
        /// because otherwise we wouldn't be able to declare such type in general and still operate with individual result instance.
        let makeOutput (f:FlowExpression<'a>) : 'a (* : ArtefactExpression<int> * ArtefactExpression<double>, for example *) =
        
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
        /// E.g. results : ArtefactExpression<int> * ArtefactExpression<double>
        let flowFromResults (results:'a) : FlowExpression<'a> =
            let asUntypedArtefact (a:obj (* : ArtefactExpression<_>*)) : UntypedArtefactExpression =
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
        let me = MethodExpression(BasicMethods.createMakeValue v, [])
        define([me], [|Single({ MethodExpr = me; Index = 0 })|])

    /// Converts any value into an artefact without binding it to an identifier.
    let value (v:'a) : ArtefactExpression<'a>  = 
        let me = MethodExpression(BasicMethods.createMakeValue v, [])
        let a:ArtefactExpression<'a> = A(Single({ MethodExpr = me; Index = 0}))
        a

    module SystemMethods =
        let id_ContractId = "id_"

        let createIdUntyped (t:System.Type) = 
                let id = id_ContractId + t.FullName
                FunctionMethod(
                    Guid.NewGuid(), 
                    { Id = id
                    ; TypeArgs = []
                    ; DisplayName = "scatter item"
                    ; Description = "Allows explicit representation of an artefact produced by a scatter method"
                    ; Inputs = [{InputContract.Type = t; InputContract.Name = "scattered array" }]
                    ; Outputs = [{OutputContract.Type = t; OutputContract.Name = "item" }] },
                    fun (args, _) ->  Seq.singleton ([args], null))

        let createId<'a>() = createIdUntyped typeof<'a>

        let isId (c:MethodContract) = c.Id.StartsWith(id_ContractId)

        /// Generates an expression which translates the given artefact as is.
        let id<'a>(a:ArtefactExpression<'a>) : FlowExpression<ArtefactExpression<'a>> = 
            let me = MethodExpression(createId<'a>(), [a.Untyped])
            define([me], [|Single({ MethodExpr = me; Index = 0 })|])

        let toScope_ContractName = "toScope_"

        let createToScopeUntyped (tScope:System.Type, tData:System.Type) = 
            let id = toScope_ContractName + tScope.FullName + "_" + tData.FullName
            FunctionMethod(
                Guid.NewGuid(), 
                { Id = id
                ; TypeArgs = []
                ; DisplayName = "to scope"
                ; Description = "Moves a method to a scope of another method"
                ; Inputs =  [{InputContract.Type = tScope; InputContract.Name = "scope origin" };
                             {InputContract.Type = tData; InputContract.Name = "data" }]
                ; Outputs = [{OutputContract.Type = tData; OutputContract.Name = "data" }] },
                fun (args, _) ->  Seq.singleton ([args.[1]], null))

        let isToScope (c:MethodContract) = c.Id.StartsWith(toScope_ContractName)

        let createToScope<'a,'b>() = createToScopeUntyped (typeof<'a>,typeof<'b>)

        /// Allows to move an artefact to a scope of another artefact.
        // Always produces "data" as its output. 
        // If first argument is a "foreach" target artefact, the method is vectorized, i.e.
        // executed for each of origin vector, and its output is a vector with same length as origin vector
        // and items are replicated "data" artefact.
        let toScope<'a,'b>(scopeOrigin:ArtefactExpression<'a>, data:ArtefactExpression<'b>) : FlowExpression<ArtefactExpression<'b>> = 
            let me = MethodExpression(createToScope<'a,'b>(), [scopeOrigin.Untyped; data.Untyped])
            define([me], [|Single({ MethodExpr = me; Index = 0 })|])

    
    /// Collects zero or more individual artefacts into an array-artefact.
    let collect<'a> (artefacts:ArtefactExpression<'a> array) : ArtefactExpression<'a array> = 
        let rec toOutput (a:ArtefactExpression<'a>) =
            match a.Untyped with 
            | Single output -> output 
            | Collection _ -> failwith "Collected array cannot be collected"
        A <| Collection(artefacts |> Seq.map toOutput |> List.ofSeq)

    module internal WorkOperations =
        open System.Collections.Generic

        type DependencyType = 
            | OneToOne | Collect of index: int
            override this.ToString() =
                match this with
                | OneToOne -> sprintf "one-to-one"
                | Collect i -> sprintf "collect %d" i

        type Dependency(fromV: MethodExpression*OutputRef, toV: MethodExpression*InputRef, dep: DependencyType) =
            interface IEdge<MethodExpression> with
                member x.Source = fst fromV
                member x.Target = fst toV
            member x.From = fromV
            member x.To = toV
            member x.Type = dep
            
            override this.ToString() =
                let m2Str (m:MethodExpression) = sprintf "%s" m.Method.Contract.DisplayName 
                sprintf "%O[%d] -- %O --> %O[%d]" (m2Str(fst fromV)) (snd fromV) dep (m2Str(fst toV)) (snd toV)

        type DependencyGraph = DirectedAcyclicGraph<MethodExpression,Dependency>

        let toGraph(methods: MethodExpression list) : DependencyGraph  = 
            let rec addMethod (g:DependencyGraph) (m:MethodExpression) : DependencyGraph = 
                if g.Vertices.Contains m then g
                else
                    let g' = g.AddVertex m
                    m.Inputs |> Seq.mapi(fun i inp -> (i,inp)) |> Seq.fold(fun g (inRef,inp) ->
                        match inp with
                        | UntypedArtefactExpression.Single output ->
                            (addMethod g output.MethodExpr).AddEdge (Dependency((output.MethodExpr, output.Index), (m, inRef), OneToOne))
                        | UntypedArtefactExpression.Collection outputs ->
                            outputs |> Seq.mapi(fun j o -> (j,o)) |> Seq.fold(fun g (colIdx,output) ->
                                (addMethod g output.MethodExpr).AddEdge (Dependency((output.MethodExpr, output.Index), (m, inRef), Collect colIdx))) g
                            ) g'
            let g' = methods |> Seq.fold addMethod (DependencyGraph())
            g'

        let ofGraph (g:DependencyGraph) : MethodExpression list =
            let oldToNew = Dictionary<MethodExpression,MethodExpression>()
            g |> toSeqSorted |> Seq.map(fun m -> 
                let indeps = g.InEdges m
                let m' =
                    let inputs : UntypedArtefactExpression list = 
                        indeps 
                        |> Seq.groupBy(fun ine -> snd ine.To)
                        |> Seq.sortBy fst
                        |> Seq.map(fun (_, inedges) -> 
                            match inedges |> Seq.toList with
                            | [] -> failwith "Computation expression doesn't allow incomplete input list"
                            | [ ine ] when (match ine.Type with OneToOne -> true | Collect _ -> false) ->
                                let fromM = oldToNew.[fst ine.From]
                                UntypedArtefactExpression.Single { MethodExpr = fromM; Index = snd ine.From }
                            | _ -> 
                                let outputs = 
                                    inedges 
                                    |> Seq.sortBy(fun ine -> match ine.Type with Collect i -> i | OneToOne -> failwith "Unexpected type of dependency" ) 
                                    |> Seq.map(fun ine ->
                                        let fromM = oldToNew.[fst ine.From]
                                        { MethodExpr = fromM; Index = snd ine.From }) |> Seq.toList
                                UntypedArtefactExpression.Collection(outputs)) |> Seq.toList
                    MethodExpression(m.Method, inputs)
                oldToNew.Add(m,m')
                m') |> Seq.toList

        let rec internal isDownstream (u: MethodExpression, d: MethodExpression, methods: MethodExpression list) : bool =
            u <> d &&
            d.Inputs |> Seq.exists(function
                | Single output -> output.MethodExpr = u || isDownstream (u, output.MethodExpr, methods)
                | Collection outputs -> outputs |> Seq.exists(fun output -> output.MethodExpr = u || isDownstream (u, output.MethodExpr, methods)))

    /// Moves output of the target method to the scope produced by the originId.
    let internal ensureTargetInScope (originIds:MethodExpression list) (methods:MethodExpression list, target:UntypedArtefactExpression) : MethodExpression list * UntypedArtefactExpression =
        let createToScope (trgOutput:Output) = 
            match originIds |> Seq.exists(fun originId -> originId = trgOutput.MethodExpr || WorkOperations.isDownstream(originId, trgOutput.MethodExpr, methods)) with
            | true -> None
            | false ->
                let dataType = trgOutput.MethodExpr.Method.Contract.Outputs.[trgOutput.Index].Type
                let origType = originIds.[0].Method.Contract.Outputs.[0].Type
                let toScope = MethodExpression(SystemMethods.createToScopeUntyped(origType,dataType), 
                                    [ UntypedArtefactExpression.Single { MethodExpr = originIds.[0]; Index = 0 } // origin
                                    ; UntypedArtefactExpression.Single { MethodExpr = trgOutput.MethodExpr; Index = trgOutput.Index }]) // data
                Some(toScope)

        match target with
        | Single trgOutput ->
            match createToScope trgOutput with
            | Some toScope -> List.append methods [toScope], (Single { MethodExpr = toScope; Index = 0 })
            | None -> methods, target
        | Collection collection ->
            let toScopes,targets = collection |> Seq.fold(fun (mthds:MethodExpression list,trgs:Output list) (trgOutput:Output) ->
                match createToScope trgOutput with
                | Some toScope -> toScope :: mthds, { MethodExpr = toScope; Index = 0 } :: trgs
                | None -> mthds, trgs) ([],[])
            methods @ toScopes, Collection targets

    /// Checks and if required connects inner foreach origin method (nestedScope) to the outer origin (originId)
    /// through "toScope" method.
    /// Type parameter is a scalar type of outer foreach, e.g. if outer foreach scatters int[] then T' is int.
    let internal nestScope<'T> (originId:MethodExpression) (methods:MethodExpression list) (nestedScope: Scope) : MethodExpression list =
        let toOrigin methods originIdNested =
            match WorkOperations.isDownstream (originId, originIdNested, methods) with
            | true -> methods
            | false ->
                let originNested, outRef = match originIdNested.Inputs.[0] with UntypedArtefactExpression.Single output -> output.MethodExpr, output.Index | _ -> failwith "Unexpected type of scope origin"
                let vectorElementType = originIdNested.Method.Contract.Inputs.[0].Type
                let dataType = originNested.Method.Contract.Outputs.[outRef].Type
                let toScope = MethodExpression(SystemMethods.createToScopeUntyped(typeof<'T>, dataType), 
                                    [ UntypedArtefactExpression.Single { MethodExpr = originId; Index = 0 } // origin
                                    ; UntypedArtefactExpression.Single { MethodExpr = originNested; Index = outRef }]) // data
                let makeArray = MethodExpression(SystemMethods.createIdUntyped(vectorElementType), // makes an array of vector to support reduce/collect of scattered vector
                                    [ UntypedArtefactExpression.Single { MethodExpr = toScope; Index = 0 } ])

                let g = (toScope :: makeArray :: methods) |> WorkOperations.toGraph
                let g1 = g.OutEdges originIdNested |> Seq.fold(fun (g:WorkOperations.DependencyGraph) (outEdge:WorkOperations.Dependency) ->
                    (g.RemoveEdge outEdge).AddEdge(WorkOperations.Dependency((makeArray,snd outEdge.From), outEdge.To, outEdge.Type))) g
                let methods = g1 |> WorkOperations.ofGraph
                methods
        nestedScope.Origin |> Seq.fold toOrigin methods

    let internal updateForeachTarget (bodyMethods:MethodExpression list) target =
        // We should update target since methods list could be changed due to nesting scopes
        let updateOutput (output:Output) : Output = 
            let m2 = bodyMethods |> Seq.find(fun m -> m.Id = output.MethodExpr.Id)
            { MethodExpr = m2; Index = output.Index }
        match target with
        | Single output -> Single (updateOutput output)
        | Collection outputs -> Collection (outputs |> List.map updateOutput)


    /// Creates a work which evaluates the given body function for each item of the given vector.
    let rec foreach(vector: ArtefactExpression<'T[]>, body: ArtefactExpression<'T> -> FlowExpression<ArtefactExpression<'U>>) : FlowExpression<ArtefactExpression<'U[]>> =
        match vector.Untyped with
        | Single scatterOutput ->
            // vector: 'T[] ==> (body: 'T->'U), Core vectorizes body: 'T[] -> 'T
            let originId = MethodExpression(SystemMethods.createId<'T>(), [ UntypedArtefactExpression.Single scatterOutput ])
            let expr : FlowExpression<ArtefactExpression<'T>> = define([originId], [|UntypedArtefactExpression.Single { MethodExpr = originId; Index = 0 }|])
            let bodyWrk = flow.Bind(expr, body)
            assert(Array.length bodyWrk.Results = 1)

            let bwMethods, bwTarget = ensureTargetInScope [originId] (bodyWrk.Methods, bodyWrk.Results.[0])

            if bwMethods = [scatterOutput.MethodExpr; originId ] && bwTarget = expr.Results.[0] then // foreach body is an identity function
                define([scatterOutput.MethodExpr], [| UntypedArtefactExpression.Single(scatterOutput) |])
            else
                let bodyMethods = bodyWrk.Scopes |> Seq.fold (nestScope<'T> originId) bwMethods
                let bodyTarget_s = updateForeachTarget bodyMethods bwTarget

                let targetId = MethodExpression(SystemMethods.createId<'U[]>(), [ bodyTarget_s ])
                { define(bodyMethods@[targetId], [| Single { MethodExpr=targetId; Index=0 } |]) with Scopes = [{ Origin = [originId]; Scopes = bodyWrk.Scopes }] }

        | Collection _ -> // Collect-Scatter
            // We add (id: 'T[] -> 'T[]) to have an explicit method gathering given artefacts and producing an array-artefact
            // which then can be connected to a body to allow Core to vectorize it, since it needs 'T[] --> 'T connection.
            // 'T -->
            // 'T --> (id: 'T[] -> 'T[]) ==> (body: 'T->'U), Core vectorizes body.
            // 'T --> 
            flow.Bind(SystemMethods.id<'T[]>(vector), fun v -> foreach(v, body))

    /// Creates a work which evaluates the given body function for each item of the given vector.
    let rec foreach2(u: ArtefactExpression<'T[]>, v: ArtefactExpression<'S[]>, body: ArtefactExpression<'T> * ArtefactExpression<'S> -> FlowExpression<ArtefactExpression<'U>>) : FlowExpression<ArtefactExpression<'U[]>> =
            match u.Untyped, v.Untyped with
            | Single scatterOutputU, Single scatterOutputV ->
                let originId_u = MethodExpression(SystemMethods.createId<'T>(), [ UntypedArtefactExpression.Single scatterOutputU ])
                let originId_v = if u.Untyped = v.Untyped then originId_u 
                                 else MethodExpression(SystemMethods.createId<'S>(), [ UntypedArtefactExpression.Single scatterOutputV ])

                let expr : FlowExpression<ArtefactExpression<'T>*ArtefactExpression<'S>> = 
                    define([originId_u; originId_v], [|UntypedArtefactExpression.Single { MethodExpr = originId_u; Index = 0 }; UntypedArtefactExpression.Single { MethodExpr = originId_v; Index = 0 }|])
                let bodyWrk = flow.Bind(expr, body)
                assert(Array.length bodyWrk.Results = 1)

                let bwMethods, bwTarget = ensureTargetInScope [originId_u;originId_v] (bodyWrk.Methods, bodyWrk.Results.[0])
            
                let bodyMethods = bodyWrk.Scopes |> Seq.fold (fun mm ss -> 
                    let mm2 = nestScope<'T> originId_u mm ss in nestScope<'S> originId_v mm2 ss) bwMethods
                let bodyTarget_s = updateForeachTarget bodyMethods bwTarget

                let targetId = MethodExpression(SystemMethods.createId<'U[]>(), [ bodyTarget_s ])
                { define(bodyMethods@[targetId], [| Single { MethodExpr=targetId; Index=0 } |]) with Scopes = [{ Origin = [originId_u;originId_v]; Scopes = bodyWrk.Scopes }] }

            | Collection _, _ ->
                flow.Bind(SystemMethods.id<'T[]>(u), fun u -> foreach2(u,v, body))
            | _, Collection _ ->
                flow.Bind(SystemMethods.id<'S[]>(v), fun v -> foreach2(u,v, body))

        /// Creates a work which evaluates the given body function for each item of the given vector.
    let rec foreach3(u: ArtefactExpression<'T[]>, v: ArtefactExpression<'S[]>, w: ArtefactExpression<'R[]>, body: ArtefactExpression<'T> * ArtefactExpression<'S> * ArtefactExpression<'R> -> FlowExpression<ArtefactExpression<'U>>) : FlowExpression<ArtefactExpression<'U[]>> =
            match u.Untyped, v.Untyped, w.Untyped with
            | Single scatterOutputU, Single scatterOutputV, Single scatterOutputW ->
                let originId_u = MethodExpression(SystemMethods.createId<'T>(), [ UntypedArtefactExpression.Single scatterOutputU ])
                let originId_v = if u.Untyped = v.Untyped then originId_u 
                                 else MethodExpression(SystemMethods.createId<'S>(), [ UntypedArtefactExpression.Single scatterOutputV ])
                let originId_w = if u.Untyped = w.Untyped then originId_u 
                                 else if v.Untyped = w.Untyped then originId_v 
                                 else MethodExpression(SystemMethods.createId<'R>(), [ UntypedArtefactExpression.Single scatterOutputW ])

                let expr : FlowExpression<ArtefactExpression<'T>*ArtefactExpression<'S>*ArtefactExpression<'R>> = 
                    define([originId_u; originId_v; originId_w], [|UntypedArtefactExpression.Single { MethodExpr = originId_u; Index = 0 }; UntypedArtefactExpression.Single { MethodExpr = originId_v; Index = 0 }; UntypedArtefactExpression.Single { MethodExpr = originId_w; Index = 0 }|])
                let bodyWrk = flow.Bind(expr, body)
                assert(Array.length bodyWrk.Results = 1)

                let bwMethods, bwTarget = ensureTargetInScope [originId_u;originId_v;originId_w] (bodyWrk.Methods, bodyWrk.Results.[0])
            
                let bodyMethods = bodyWrk.Scopes |> Seq.fold (fun mm ss -> 
                    let mm2 = nestScope<'T> originId_u mm ss 
                    let mm3 = nestScope<'S> originId_v mm2 ss
                    nestScope<'R> originId_w mm3 ss) bwMethods
                let bodyTarget_s = updateForeachTarget bodyMethods bwTarget

                let targetId = MethodExpression(SystemMethods.createId<'U[]>(), [ bodyTarget_s ])
                { define(bodyMethods@[targetId], [| Single { MethodExpr=targetId; Index=0 } |]) with Scopes = [{ Origin = [originId_u;originId_v;originId_w]; Scopes = bodyWrk.Scopes }] }

            | Collection _, _, _ ->
                flow.Bind(SystemMethods.id<'T[]>(u), fun u -> foreach3(u,v,w, body))
            | _, Collection _, _ ->
                flow.Bind(SystemMethods.id<'S[]>(v), fun v -> foreach3(u,v,w, body))
            | _, _, Collection _ ->
                flow.Bind(SystemMethods.id<'R[]>(w), fun w -> foreach3(u,v,w, body))

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

    let internal isOptimizable (v:Method) = 
        SystemMethods.isId v.Contract

    
    /// Removes automatically added id methods (represented by 'scopes') if they are not needed
    /// Rules:
    ///    scatter -> id -> o2o     ==> scatter, OR
    ///    scatter -> id -> reduce  ==> o2o,     OR
    ///    o2o     -> id -> o2o     ==> o2o              (e.g. foreach is applied to result of another foreach)
    ///    reduce  -> id -> o2o     ==> reduce
    ///    reduce  -> id -> scatter ==> o2o
    ///
    /// Remarks: scatter -> id -> collect, scatter -> id -> scatter cannot be replaced with single edge
    let rec internal build_x_optimize_ids (work : build_x_state) (ids: Method list): build_x_state =
        let rec tryOptimize_id (work : build_x_state) v_id (inEdge : Edge<Method>) outEdgeCriteria : build_x_state  =          
            let removeId work (v_id:Method) = 
                { IdToMethod = work.IdToMethod.Remove v_id.Id
                ; State = 
                    { Graph = work.State.Graph.TryRemove v_id |> Option.get
                    ; Vertices = work.State.Vertices.Remove v_id 
                    ; TimeIndex = work.State.TimeIndex }
                }

            let outEdges = work.State.Graph.Structure.OutEdges v_id |> Seq.toList
            let batch = outEdges |> List.choose(fun outEdge ->
                    if outEdgeCriteria outEdge then
                        let repeatIds = seq {
                                            if isOptimizable inEdge.Source then yield inEdge.Source
                                            if isOptimizable outEdge.Target then yield outEdge.Target
                                        } |> Seq.toList
                        // connecting 'in' and 'out'-s directly
                        Some((outEdge.Target, outEdge.InputRef, outEdge.Source, outEdge.OutputRef), // disconnect
                             (outEdge.Target, outEdge.InputRef, inEdge.Source, inEdge.OutputRef), // connect
                             repeatIds)  
                    else None)
            if batch.Length > 0 then 
                let work' = 
                    { work with State = { work.State with Graph = work.State.Graph.BatchAlter (batch |> List.map (fun (dc,_,_) -> dc)) (batch |> List.map (fun (_,cn,_) -> cn)) } }
                let rep = batch |> Seq.map(fun (_,_,rep) -> rep) |> Seq.concat |> Seq.distinct |> Seq.toList
                let work'' = if outEdges.Length = batch.Length then removeId work' v_id else work'                    
                build_x_optimize_ids work'' rep // allows to optimize chains of ids
            else if outEdges.Length = 0 then
                let work' = removeId work v_id 
                if isOptimizable inEdge.Source then build_x_optimize_ids work' [inEdge.Source] else work'
            else work 

        let build_x_optimize_id (work : build_x_state) (v_id: Method) =
            let graph = work.State.Graph          
            match graph.Structure.Vertices.Contains v_id with
            | true -> // vertex still exists 
                let inEdges = graph.Structure.InEdges v_id |> Seq.toArray
                match inEdges.Length, inEdges.[0].Type with 
                | 1, Scatter  _ -> tryOptimize_id work v_id inEdges.[0] (fun outEdge -> match outEdge.Type with OneToOne _ | Reduce _ -> true | Scatter _ | Collect _             -> false)
                | 1, OneToOne _ -> tryOptimize_id work v_id inEdges.[0] (fun outEdge -> match outEdge.Type with OneToOne _            -> true | Reduce _  | Scatter _ | Collect _ -> false)
                | 1, Reduce   _ -> tryOptimize_id work v_id inEdges.[0] (fun outEdge -> match outEdge.Type with OneToOne _ | Scatter _-> true | Reduce _  | Collect _             -> false)
                | _ -> work // do not change anything            
            | false -> work // already optimized
        ids |> List.fold build_x_optimize_id work     


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
        let ids = work1.State.Graph.Structure.Vertices |> Set.filter(fun v -> isOptimizable v) |> Set.toList
        let work2 = build_x_optimize_ids work1 ids

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
                        if SystemMethods.isId output.MethodExpr.Method.Contract then find output.MethodExpr.Inputs.[0]
                        else failwith "Cannot find a vertex representing a work evaluation target"
                | Collection _ -> failwith "Cannot evaluate a collection of artefacts"
        f.Results |> Array.map find

    let run (def:FlowExpression<ArtefactExpression<'result>>) : 'result = 
        assert(1 = def.Results.Length)

        let initial = build def
        let final = Control.runToFinal initial

        let m = findResultMethods def final.Graph 
        final |> Control.outputScalar m.[0] 
        
    let run2 (def:FlowExpression<ArtefactExpression<'a>*ArtefactExpression<'b>>) : 'a*'b = 
        assert(2 = def.Results.Length)

        let initial = build def
        let final = Control.runToFinal initial

        let m = findResultMethods def final.Graph         
        let a = final |> Control.outputScalar m.[0] 
        let b = final |> Control.outputScalar m.[1] 
        a,b

    let run3 (def:FlowExpression<ArtefactExpression<'a>*ArtefactExpression<'b>*ArtefactExpression<'c>>) : 'a*'b*'c = 
        assert(3 = def.Results.Length)

        let initial = build def
        let final = Control.runToFinal initial

        let m = findResultMethods def final.Graph         
        let a = final |> Control.outputScalar m.[0] 
        let b = final |> Control.outputScalar m.[1] 
        let c = final |> Control.outputScalar m.[2] 
        a,b,c