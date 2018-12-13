.. _WMSArchitecture:

=======================================
Workload Management System architecture
=======================================

The WMS is a standard DIRAC system, and therefore it is composed by components in the following categories: Services, DBs, Agents, but also Executors.


* **DB**

  * JobDB:
  Main WMS database containing job definitions and status information. It is used in most of the WMS components.

  * JobLoggingDB:
  Simple Job Logging Database.

  * PilotAgentsDB: 
  Keeps track of all the submitted grid pilot jobs. It also registers the mapping of the DIRAC jobs to the pilots.

  * SandboxMetadataDB:
  Keeps the metadata of the sandboxes

  * TaskQueueDB:
  The TaskQueueDB is used to organize jobs requirements into task queues, for easier matching.

All the DB above should be installed using the :ref:`system administrator console <system-admin-console>`.


* **Services**

  * JobManager:
    For submitting/rescheduling/killing/deleting jobs

  * JobMonitoring:
    For monitoring jobs

  * Matcher:
    For matching capabilities (of WNs) to requirements (of task queues --> so, of jobs)

  * JobStateUpdate:
    For storing updates on Jobs' status

  * OptimizationMind:
    For Jobs scheduling optimization

  * SandboxStore:
    Frontend for storing and retrieving sandboxes

  * WMSAdministrator:
    For administering jobs and pilots

All these services are necessary for the WMS. Each of them should be installed using the :ref:`system administrator console <system-admin-console>`.
You can have several instances of each of them running, with the exclusion of the Matcher and the OptimizationMind [TBC].

* **Agents**

  * SiteDirector:
  send pilot jobs to Sites/CEs/Queues

  * JobCleaning:
  cleans old jobs from the system

  * PilotStatus
  updates the status of the pilot jobs on the PilotAgentsDB

  * StalleJobAgent
  hunts for stalled jobs in the Job database. Jobs in "running" state not receiving a heart beat signal for more than stalledTime seconds will be assigned the "Stalled" state.

All these agents are necessary for the WMS, with the exclusion of the . Each of them should be installed using the :ref:`system administrator console <system-admin-console>`.
You can duplicate some of these agents as long as you provide the correct configuration.
A typical example is the SiteDirector, for which you may want to deploy even 1 for each of the sites managed.

Optional agents are:

  * StatesAccounting or StatesMonitoring
  produce monitoring plots then found in Accounting. Use one or the other.

A very different type of agent is the *JobAgent*, which is run by the pilot jobs and should NOT be run in a server installation.


* **Executors**

  * Optimizer
  optimizer for jobs submission and scheduling.

All these services are necessary for the WMS. Each of them should be installed using the :ref:`system administrator console <system-admin-console>`.
