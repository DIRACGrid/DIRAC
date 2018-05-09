----------------------
FTS transfers in DIRAC
----------------------

DIRAC DMS can be configured to make use of FTS servers in order to schedule and monitor efficient transfer of large amounts of data between SEs. As of today, FTS servers are only able to handle transfers between SRM SEs.

The transfers using FTS come from the RequestManagementSystem ( see :ref:`requestManagementSystem`). It will receive the files to transfer, as well as the list of destinations. If no source is defined, it will choose one. The files will then be grouped together and submited as jobs to the fts servers. These jobs will be monitored, retried if needed, the new replicas will be registered, and the status of the files will be reported back to the RMS.

There are no direct submission possible to the FTS system, it has to go through the RMS.

In the current system, only the production files can be transfered using FTS, sine the transfers are done using the Shifter proxy


Enable FTS transfers in the RMS
-------------------------------

In order for the transfers to be submitted to the FTS system:

   * `Systems/RequestManagementSystem/Agents/RequestExecutingAgent/OperationHandlers/ReplicateAndRegister/FTSMode` must be True
   * `Systems/RequestManagementSystem/Agents/RequestExecutingAgent/OperationHandlers/ReplicateAndRegister/FTSBannedGroups` should contain the list of groups that are not production groups (users, etc)

Operations configuration
------------------------

  * DataManagement/FTSVersion: FTS2/FTS3. Set it to FTS3...
  * DataManagement/FTSPlacement/FTS3/ServerPolicy: Policy to choose the FTS server see below


FTS servers definition
----------------------

The servers to be used are defined in the `Resources/FTSEndpoints/FTS3` section. Example:

.. code-block:: python

  CERN-FTS3 = https://fts3.cern.ch:8446
  RAL-FTS3 = https://lcgfts3.gridpp.rl.ac.uk:8446

The option name is just the server name as used internaly. Note that the port number has to be specified, and should correspond to the REST interface


Components
----------

The list of components you need to have installed is:

   * FTSDB: guess...
   * FTSManager: just the interface to the DB
   * FTSAgent: this agent runs the whole show
   * CleanFTSDBAgent: cleans up the database from old jobs.


FTSDB
-----

Two tables:

   * FTSFile: an LFN and a destination SE, potentially a source SE, the metadata of the LFN, and the relevant IDs to make the link with the RMS. Also an link to the FTSJob table if they are currently being transfered.
   * FTSJob: a job submitted to the FTS servers


FTSManager
----------

No specific configuration for that one


CleanFTSDBAgent
---------------

This agent is responsible for cleaning the database from old jobs. Besides the usual agent options, these are the possible configurations:

  * DeleteGraceDays: number of days after we remove a job in final status
  * DeleteLimitPerCycle: maximum number of jobs we delete per agent cycle


FTSAgent
--------

This is the complex one. The agent is going to fetch the request in state `Scheduled` in the RMS, and look in the FTSDB for the associated FTSFiles. It is then going to monitor submitted jobs, submit new jobs with new files or files that failed previously, register files successfuly transfered

The agent still supports old FTS2 server, but since there are no such servers anymore, this behavior will not be detailed here.


Configuration options
^^^^^^^^^^^^^^^^^^^^^


  * FTSPlacementValidityPeriod: deprecated (FTS2)
  * MaxActiveJobsPerRoute: deprecated (FTS2)
  * MaxFilesPerJob: maximum number of files in a single fts job
  * MaxRequests: maximum number of requests to look at per agent's cycle
  * MaxThreads: maximum number of threads
  * MaxTransferAttempts: maximum number of time we attempt to transfer a file
  * MinThreads: minimum number of threads
  * MonitorCommand: deprecated (FTS2)
  * MonitoringInterval: interval between two monitoring of an FTSJob (in second)
  * PinTime: when staging, pin time requested in the FTS job (in second)
  * ProcessJobRequests: True if this agent is meant to process job only transfers (see `Multiple FTSAgents`_)
  * SubmitCommand: deprecated (FTS2)





File registration
^^^^^^^^^^^^^^^^^

The FTSAgent runs with the DataManagement shifter proxy, and hense can register the files directly after they have been transfered. If the registration fails, the FTSAgent still considers the transfer as done, and adds a RegisterFile operation in the RMS Request from which the transfers originated


.. _multipleFTSAgents:

Multiple FTSAgents
^^^^^^^^^^^^^^^^^^

It is not possible to have several FTSAgents running in parallel except in a very specific configuration, which is 1 agent taking care of the failover transfers, 1 agent taking care of the transformation transfers. This behavior is enabled by the `ProcessJobRequests` flag. But be careful, two agents taking care of the same case would lead to problems.

Without entering the details on how to install several instances of the same agent, if you want such a configuration, it would look something like.

.. code-block:: python

   FTSAgent
   {
     # All the common options
   }

   FTSAgentTransformations
   {
     Module = FTSAgent
     ProcessJobRequests = False
     ControlDirectory = control/DataManagement/FTSAgentTransformations
     # whatever other options
     # ...
   }

   FTSAgentFailover
   {
     Module = FTSAgent
     ProcessJobRequests = True
     ControlDirectory = control/DataManagement/FTSAgentFailover
     # whatever other options
     # ...
   }


FTSServer policy
^^^^^^^^^^^^^^^^

The FTS server to which the job is sent is chose based on the policy. There are 3 possible policy:

  * Random: the default. makes a random choice
  * Failover: pick one, and stay on that one until it fails
  * Sequence: take them in turn, always change
