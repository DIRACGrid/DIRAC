.. _WMSArchitecture:

=======================================
Workload Management System architecture
=======================================

The WMS is a standard DIRAC system, and therefore it is composed by components in the following categories: Services, DBs, Agents, but also Executors.


Databases
---------

JobDB
  Main WMS database containing job definitions and status information. It is used in most of the WMS components.

JobLoggingDB
  Simple Job Logging Database.

PilotAgentsDB
  Keep track of all the submitted grid pilot jobs. It also registers the mapping of the DIRAC jobs to the pilots.

SandboxMetadataDB
  Keep the metadata of the sandboxes.

TaskQueueDB
  The TaskQueueDB is used to organize jobs requirements into task queues, for easier matching.

All the DB above should be installed using the :ref:`system administrator console <system-admin-console>`.


Services
--------

JobManager
  For submitting/rescheduling/killing/deleting jobs

JobMonitoring
  For monitoring jobs

Matcher
  For matching capabilities (of WNs) to requirements (of task queues --> so, of jobs)

JobStateUpdate
  For storing updates on Jobs' status

OptimizationMind
  For Jobs scheduling optimization

SandboxStore
  Frontend for storing and retrieving sandboxes

WMSAdministrator
  For administering jobs and pilots

All these services are necessary for the WMS. Each of them should be installed using the :ref:`system administrator console <system-admin-console>`.
You can have several instances of each of them running, with the exclusion of the Matcher and the OptimizationMind [TBC].

Agents
------

SiteDirector
  send pilot jobs to Sites/CEs/Queues

JobCleaningAgent
  clean old jobs from the system

PilotStatusAgent
  update the status of the pilot jobs on the PilotAgentsDB

StalledJobAgent
  hunt for stalled jobs in the Job database. Jobs in "running" state not receiving a heart beat signal for more than stalledTime seconds will be assigned the "Stalled" state.

All these agents are necessary for the WMS, and each of them should be installed using the :ref:`system administrator console <system-admin-console>`.
You can duplicate some of these agents as long as you provide the correct configuration.
A typical example is the SiteDirector, for which you may want to deploy even 1 for each of the sites managed.

Optional agents are:

StatesAccountingAgent or StatesMonitoringAgent
  produce monitoring plots then found in Accounting. Use one or the other.

A very different type of agent is the *JobAgent*, which is run by the pilot jobs and should NOT be run in a server installation.


Executors
---------

Optimizers
  optimize job submission and scheduling. The four executors that are run by default are: InputData, JobPath,
  JobSanity, JobScheduling. The ``Optimizers`` executor is a wrapper around all executors that are to be run. The executor modules
  it will run is given by the ``Load`` configuration option.


The ``Optimizers`` executor is necessary for the WMS. It should be installed using the :ref:`system administrator console
<system-admin-console>` and it can also be duplicated.

To run additional executors inside the ``Optimizers`` executor change its ``Load`` parameter in the CS or during the
installation with the :ref:`system administrator console <system-admin-console>`::

  install executor WorkloadManagement Optimizers -p Load=JobPath,JobSanity,InputData,MyCustomExecutor,JobScheduling

For detailed information on each of these components, please do refer to the WMS :ref:`Code Documentation<code_documentation>`.
