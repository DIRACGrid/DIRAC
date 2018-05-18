.. _rmsConcepts:

--------
Concepts
--------

Requests, Operations, Files
---------------------------

At the core of the RMS are `Requests`, `Operations` and `Files`.

A `Request` is like a TODO list associated to a User and group. For example, this TODO list could be what is left to do at the end of a job (setting the job status, moving the output file to its final destination, etc).

Each item on this TODO list is described by an `Operation`. There are several types of `Operation`, for example `ReplicateAndRegister` (to copy a file), `RemoveFile` (guess..), `ForwardDISET` (to execute DISET calls), etc.

When an `Operation` acts on LFNs, `Files` corresponding to the LFNs are associated to the `Operation`.

The list of available `Operations`, as well as the state machines are described in :ref:`rmsObjects`

ReqManager & ReqProxy
---------------------

The `ReqManager` is the service that receives or distributes `Requests` to be excuted. Every operation is synchronous with the `ReqDB` database.

If the ReqManager is unreachable when a client wants to send a `Request`, the client will automatically failover to a `ReqProxy`. This proxy will accept the `Request`, hold it in a local cache, and will periodically try to send it to the `ReqManager` until it succeeds. This system ensures that no Request is lost.

RequestExecutingAgent
---------------------

The `RequestExecutingAgent` (:ref:`RequestExecutinAgent`) is in charge of executing the `Requests`.

CleanReqDBAgent
---------------

Because the database can grow very large, the :ref:`CleanReqDBAgent` is in charge of removing old `Requests` in a final state.
