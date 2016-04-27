Angara.Execution
================

The module contains runtime that evaluates the dataflow methods. 
To distinct the data flow method, we will write the word with the capital letter, Method.

A *Method is a reusable routine* for model specification, simulation, transformation or analysis.
Upon completion each Method produces one or more Artefacts, immutable data structures 
representing the result of the simulation, transformation or analysis.

A Method shall *not be very simple*. Its result shall convey meaningful information about 
a model or data. For example, "addition of two numbers" is not a good method candidate as 
one must perform thousands and millions of such trivial operations to produce something interesting.

A Method shall be *generic* enough to be used in multiple research projects. 
For example, a routine of MCMC sampling some built-in likelihood function is not a good 
Method candidate. Proper Method shall have a way to construct the sampled likelihood 
functions depending on the needs of particular project.

A Method MUST be *deterministic*. When run for the second time with the same set of inputs 
it must produce the same Artefact. For example, when a method employs random numbers 
it must have private instance of random number generator and a way to deterministically 
initialize (seed) the generator. Exceptions to the rule are Methods that contact external 
files/services where it is not possible to ensure 100% deterministic behaviour. 

Each Method has a specific *contract* which determines exact number and types 
of the inputs and outputs. Each Method may require zero or more inputs and outputs. 
The contract cannot be changed in runtime. 

Method Types
------------

Basically Angara.Flow supports following types of methods:

* *Function*: the Method is a synchronous function which takes input artefacts and returns another artefacts 
  corresponding to its contract. Evaluation of such method is invocation of the function
  and using the function results as the method output artefacts.

* *Iterative function*: the Method is a synchronous function which takes input artefacts and returns a sequence
  of artefacts. Each element of the sequence is considered as an output of the Method. The state
  machine has the corresponding status `Started(k)` where `k` is a count of iterations completed. Note that 
  state with status `Started(k)` for `k > 0` is considered as up-to-date so the depending Methods can start as well.
   
  If the Iterative Method starts from the `Complete(k)` state when `k > 0`, it *resumes* from the 
  iteration `k`; it means that the Runtime ignores outputs of the first `k` iterations.  
   
* *Resumable iterative function*: the Method is a synchronous function which takes input artefacts
  and an optional array of artefacts, which is a result of some iteration of this Method.
  The latter allows the Method to start evaluation from the iteration next to that for which the
  optional array is given.
  
* *Compound method*: the Method is defined as a dataflow. The input artefacts are bound to 
  some free input ports of the dataflow; some output ports of the dataflow are bound to the Method's
  outputs. 
  Evaluation of the Method is execution of the dataflow, which can be iterative as well.
  The inner dataflow state is stored as a part of the vertex data along with the output artefacts.
  


Method Evaluator
----------------

The Runtime uses the Method Evaluator to evaluate a Method.

Compound Method Inner Runtime
-----------------------------

This feature allows to subscribe to dynamic state of the Runtime that evaluates the dataflow of 
a compound method.

Context
-------

The Method routine should use the Context to report progress and get the cancellation token.
