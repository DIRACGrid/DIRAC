.. _rmsObjects:

===========
RMS objects
===========

Requests
========

A `Request` is like a TODO list, each of the task being an `Operation`. A `Request` has the following attributes:

* `CreationTime`: Time at which the Request was created
* `Error`: Error if any at execution
* `JobID`: The ID of the job that generated the request. If it comes from another source, it is 0
* `LastUpdate`: Last time the request was touched
* `NotBefore`: Time before which no execution will be attempted (automatic increments in case of failures, or unavailable SE)
* `OwnerDN`: DN of the owner of the request
* `OwnerGroup`: group of the owner of the request
* `RequestID`: Unique identifier of the request
* `RequestName`: Convenience name, does not need to be unique
* `SourceComponent`: Who/what created the request (unused)
* `Status`: Status of the request (see :ref:`rmsStateMachine`)
* `SubmitTime`: Time at which the request was submitted to the database

And of course, it has an ordered list of `Operations`


Operations
==========

An `Operation` is a task to execute. It is ordered within its `Request`

* `Arguments`: Generic blob used as placeholder for some Operation types
* `Catalog`: If an operation should target specific :ref:`dmsCatalog` only
* `CreationTime`: Time at which the Operation was created
* `Error`: Error if any at execution
* `LastUpdate`: Last time the Operation was touched
* `Order`: Execution order within the Request
* `SourceSE`: Coma separated list of StorageElement used as source (used by some Operation types)
* `Status`: Status of the Operation (see :ref:`rmsStateMachine`)
* `SubmitTime`: Time at which the Operation was submitted to the database
* `TargetSE`: Coma separated list of StorageElement used as target (used by some Operation types)
* `Type`: Type of Operation (see :ref:`rmsOpType`)

In some cases, an `Operation also has a list of `Files` associated to it

Files
=====

A `File` represents an LFN. Not all the `Operations` have `Files`. `Files'` attributes are

* `Attempt`: Number of time the `Operation` was attempted on that file
* `Checksum`: Checksum of the file
* `ChecksumType`: always `Adler32`
* `Error`:  Error if any at execution
* `GUID`: file's GUID
* `LFN`: file's LFN
* `PFN`: file's URL, unused in practice
* `Size`: size of the file
* `Status`: Status of the File (see :ref:`rmsStateMachine`)

.. _rmsStateMachine:

RMS state machine
=================

The objects in the RMS obey a state machine in their execution. Each of them can have different statuses. The status of a `File` is determined by the success of the action we attempt to perform. The status of the Operation is inferred from the Files (if it has any, otherwise from the success of the execution). The status of the Request is inferred from the Operations

-------
Request
-------

  .. image:: ../../../_static/Systems/RMS/RequestSTM.png
     :alt: State machine for Request.
     :align: center

There are two special states for a Request:

* `Assigned`: this means that it has been picked up by a RequestExecutingAgent for execution
* `Canceled`: this means that we should stop trying. A Request can only be put manually in that state, and will remain as such (even if it was held by a RequestExecutingAgent, and set back)

---------
Operation
---------

  .. image:: ../../../_static/Systems/RMS/OperationSTM.png
     :alt: State machine for operation.
     :align: center

----
File
----

  .. image:: ../../../_static/Systems/RMS/FileSTM.png
     :alt: State machine for File.
     :align: center



.. _rmsOpType:

Operation types
===============

Each of this Type correspond to what can be found in the `Type` field of an `Operation`. In order to be executed, they need to be entered in the CS under `/Systems/RequestManagementSystem/Agents/RequestExecutingAgent/OperationHandlers`. Each of the Type must have its own section named after the type (for example `/Systems/RequestManagementSystem/Agents/RequestExecutingAgent/OperationHandlers/ReplicateAndRegister`)

The OperationHandler sections share a few standard arguments:

 * `Location`: (mandatory) Path (without .py) in the pythonpath to the handler
 * `LogLevel`: self explanatory
 * `MaxAttempts` (default 1024): Maximum attempts to try an Operation, after what, it fails. Note that this only works for Operations with `Files` (the others are tried forever).
 * `TimeOut`: base timeout of the Operation
 * `TimeOutPerFile`: additional timeout per file

 If `TimeOut` is not specified, the default timeout of the RequestExecutingAgent is used. Otherwise, the total timeout when executing an operation is calculated with `TimeOut + NbOfFiles * TimeOutPerFile`


For more information on how to add new Operation type, see :ref:`devRMS`

-------------------------
DataManagement Operations
-------------------------

For these operations, the `SourceSE`, `TargetSE` and `Catalog` fields of an `Operation` are used


MoveReplica
-----------

This handler moves replicas from source SEs to target SEs.

Details: :py:mod:`~DIRAC.DataManagementSystem.Agent.RequestOperations.MoveReplica`

No specific configuration options


PutAndRegister
--------------

Put a local file on an SE and registers it. This is very useful for example to move data from the experiment site to the grid world.


Details: :py:class:`~DIRAC.DataManagementSystem.Agent.RequestOperations.PutAndRegister.PutAndRegister`

No specific configuration options


RegisterFile
------------

Register files in the FileCatalogs

Details: :py:mod:`~DIRAC.DataManagementSystem.Agent.RequestOperations.RegisterFile`


RegisterReplica
---------------

Register a replica in the FileCatalogs

Details: :py:mod:`~DIRAC.DataManagementSystem.Agent.RequestOperations.RegisterReplica`


RemoveFile
----------

Remove a file from all SEs and FC

Details: :py:mod:`~DIRAC.DataManagementSystem.Agent.RequestOperations.RemoveFile`


RemoveReplica
-------------

Remove the replica of a file at a given SE and from the FC


Details: :py:mod:`~DIRAC.DataManagementSystem.Agent.RequestOperations.RemoveReplica`


ReplicateAndRegister
--------------------

This Operation replicates a file to one or several SE. The source does not need to be specified, but can be. This is typically useful in case of failover: if a job tries to upload a file to its final destination and fails, it will upload it somewhere else, and creates a `ReplicateAndRegister` Operation as well as a `RemoveReplica` (from the temporary storage) Operation. The replication can be performed either locally, or delegating it to the FTS system (:ref:`fts3`)

Details: :py:mod:`~DIRAC.DataManagementSystem.Agent.RequestOperations.ReplicateAndRegister`

Extra configuration options:

* `FTSMode`: If True, will use FTS to transfer files
* `FTSBannedGroups` : list of groups for which not to use FTS

------
Others
------



ForwardDISET
------------

The ForwardDISET operation is an operation allowing to execute a DISET RPC call on behalf of another user. Typically, when a datamanagement operation is performed, some accounting information are sent to the DataStore service. If this service turns out to be unavailable, a `Request` containing a `ForwardDISET` Operation will be created, that will just replay the exact same action.

Details: :py:mod:`~DIRAC.RequestManagementSystem.Agent.RequestOperations.ForwardDISET`


SetFileStatus
-------------

This `Operation` is used as a failover by jobs to set the status of a File in a Transformation.

:py:mod:`~DIRAC.TransformationSystem.Agent.RequestOperations.SetFileStatus`
