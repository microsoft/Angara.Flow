module internal Angara.Observable

open System
open System.Diagnostics

[<ReflectedDefinition>]
[<Interface>]
type IObservableSource<'T> =
    abstract member Next: 'T -> unit
    abstract member Completed: unit -> unit
    abstract member Error: exn -> unit

    abstract member AsObservable : IObservable<'T>


//Object expression not supported by quotations, so promoted to explicit type
[<ReflectedDefinition>]
type DisposableThreadSafeObservable<'T>(thisLock, subscriptions: Ref<Map<int, IObserver<'T>>>, key1) =
    interface IDisposable with
        member this.Dispose() = 
            lock thisLock (fun () -> 
                subscriptions := (!subscriptions).Remove(key1))

[<ReflectedDefinition>]
type ThreadSafeObservable<'T>(thisLock, key: Ref<int>, subscriptions: Ref<Map<int, IObserver<'T>>>) =
    interface IObservable<'T> with
        member this.Subscribe(obs) =
            let key1 =
                lock thisLock (fun () ->
                    let key1 = !key
                    key := !key + 1
                    subscriptions := (!subscriptions).Add(key1, obs)
                    key1)
            new DisposableThreadSafeObservable<'T>(thisLock, subscriptions, key1) :> IDisposable

[<ReflectedDefinition>]
type ObservableSource<'T>() =

    let protect function1 =
        let mutable ok = false
        try 
            function1()
            ok <- true
        with ex ->
            Debug.Assert(ok, "IObserver method threw an exception: " + (ex.ToString()))

    let key = ref 0

    // Use a Map, not a Dictionary, because callers might unsubscribe in the OnNext
    // method, so thread-safe snapshots of subscribers to iterate over are needed.
    let subscriptions = ref Map.empty : Ref<Map<int, IObserver<'T>>>

    let next(obs) = 
        !subscriptions |> Seq.iter (fun (KeyValue(_, value)) -> 
            protect (fun () -> value.OnNext(obs)))

    let completed() = 
        !subscriptions |> Seq.iter (fun (KeyValue(_, value)) -> 
            protect (fun () -> value.OnCompleted()))

    let error(err) = 
        !subscriptions |> Seq.iter (fun (KeyValue(_, value)) -> 
            protect (fun () -> value.OnError(err)))

    let thisLock = new obj()

    let mutable finished = false

    // The source ought to call these methods in serialized fashion (from
    // any thread, but serialized and non-reentrant).
    member this.Next(obs) =
        Debug.Assert(not finished, "IObserver is already finished")
        next obs

    member this.Completed() =
        Debug.Assert(not finished, "IObserver is already finished")
        finished <- true
        completed()

    member this.Error(err) =
        Debug.Assert(not finished, "IObserver is already finished")
        finished <- true
        error err

    member this.Finished = finished

    // The IObservable object returned is thread-safe; you can subscribe 
    // and unsubscribe (Dispose) concurrently.
    //member this.AsObservable = obs
    member this.AsObservable : IObservable<'T> = upcast ThreadSafeObservable(thisLock, key, subscriptions)

    interface IObservableSource<'T> with
        member x.AsObservable: IObservable<'T> = 
            x.AsObservable
        
        member x.Completed(): unit = 
            x.Completed()
        
        member x.Error(err: exn): unit = 
            x.Error(err)
        
        member x.Next(arg: 'T): unit = 
            x.Next(arg)
