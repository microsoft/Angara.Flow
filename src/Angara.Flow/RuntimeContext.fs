/// Implements a simple run time environment.
///
/// A long-running function may choose to communicate with
/// its environment for the purpose of:
/// - checking for cancellation requests;
/// - reporting the computation progress towards its completion;
/// - reporting textual computation trace messages.
///
/// This module provides a default environment as well allows for
/// specific environments for each running thread using thread-local store.
module Angara.RuntimeContext 

open System

/// Context value type.
type ContextType = {
    Token : Threading.CancellationToken
    ProgressReporter : IProgress<float>
    Logger : ILogger
    }
/// An abstract logger with three levels of details.
and [<Interface>] ILogger =
    /// Reports fine-grained informational messages that are most useful to debug a computation.
    abstract member Debug : string -> unit
    /// Reports informational messages that highlight the progress of the computation at coarse-grained level. 
    abstract member Info : string -> unit
    /// Reports abnormal situations that still allow the computation to continue.
    abstract member Error : string -> unit
    /// An expected level of details.
    abstract member Level : LogLevel
/// The expected level of details. 
and LogLevel = Error | Info | Debug 

// Default context value.
type private Reporter() = interface IProgress<float> with member x.Report f = ()
type private Logger() =
    interface ILogger with
        member x.Debug msg = ()
        member x.Info msg = printfn "%s" msg
        member x.Error msg = printfn "%s" msg
        member x.Level = Info

let private empty  = {Token = Async.DefaultCancellationToken; ProgressReporter = Reporter(); Logger = Logger()}


/// Implementation of a thread static field.
type private ThreadContextImpl =
    [<ThreadStatic; DefaultValue>]
    static val mutable private current : ContextType option
    static member Current =
        match ThreadContextImpl.current with None -> empty | Some context -> context
    static member Replace context =
        let previous = ThreadContextImpl.current
        ThreadContextImpl.current <- Some context
        match previous with None -> empty | Some context -> context

/// Returns current context for the thread.
let getContext() = ThreadContextImpl.Current

/// Installs new context in the thread and returns previous context
let replaceContext = ThreadContextImpl.Replace

/// Formats a message and reports it as an information
let tracef format = 
    let trace msg = getContext().Logger.Info msg
    Printf.kprintf trace format

/// Tells an execution environment a progress (between 0 and 1) of running computation.
let reportProgress f = getContext().ProgressReporter.Report f

/// Returns cancellationToken set for current thread.
let getCacellationToken() = getContext().Token

/// Reports a message for debugging purposes.
let logDebug msg = getContext().Logger.Debug msg

/// Reports an informational message that helps trace computation.
let logInfo msg = getContext().Logger.Info msg

/// Reports an error message.
let logError msg = getContext().Logger.Error msg