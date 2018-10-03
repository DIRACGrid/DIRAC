.. contents:: Table of contents
   :depth: 3

===========================
Workload Management System
===========================

------------
Overview
------------

The system provides high user jobs efficiency, hiding the heterogeneity of the the underlying computing resources.

It realizes the task scheduling paradigm with Generic Pilot Jobs (or Agents). 
This task scheduling method solves many problems of using unstable distributed computing resources which are available in computing grids.

------------
Architecture
------------

It is based on layered architecture and is based on DIRAC architecture:

* **Services**

  * JobManagerHandler
  * JobMonitoringHandler
  * JobPolicy
  * JobStateSyncHandler
  * JobStateUpdateHandler
  * MatcherHandler
  * OptimizationMindHandler
  * PilotsLoggingHandler
  * SandboxStoreHandler
  * WMSAdministratorHandler
  * WMSUtilities

* **DB**

  * JobDB:
    JobDB class is a front-end to the main WMS database containing job definitions and status information.
    It is used in most of the WMScomponents and is based on MySQL.

  * JobLoggingDB:
    JobLoggingDB class is a front-end to the Job Logging Database and based on MySQL.

  * PilotAgentsDB:
    PilotAgentsDB class is a front-end to the Pilot Agent Database.
    This database keeps track of all the submitted grid pilot jobs.
    It also registers the mapping of the DIRAC jobs to the pilot agents.

  * PilotsLoggingDB:
    PilotsLoggingDB class is a front-end to the Pilots Logging Database.
    This database keeps track of all the submitted grid pilot jobs.
    It also registers the mapping of the DIRAC jobs to the pilot agents.

  * SandboxMetadataDB
    SandboxMetadataDB class is a front-end to the metadata for sandboxes.

  * ElasticJobDB
    JobDB class is a front-end to the main WMS database containing job definitions and status information. 
    It is used in most of the WMS components and is based on ElasticSearch.
    
------------------------------------------
Using ElasticSearch DB for Job Parameters 
------------------------------------------

ElasticJobDB is a DB class which is used to interact with ElasticSearch backend. It contains methods
to retreive (get) information about the Job Parameters along with updating and creating those parameters.

The class consists of two main methods:

  * getJobParameters(JobID, ParamList (optional)): 
    This method can be used to get information of the Job Parameters based on the JobID. Returns name and value.
    Optional ParamList can be given to make the search more specific.
    The method uses the search API provided by ElasticSearch-py.

  * setJobParameter(JobID, Name, Value):
    This method is used to update the Job Parameters based on the given JobID. Returns result of the operation.
    If JobID  is not present in the index, it inserts the given values in that day's index.
    The method uses the update-by-query and create APIs provided by ElasticSearch-py.

The indexes are created on a daily basis and with a prefix 'jobelasticdb'. Format of the index name is:
'jobelasticdb-year-month-date'.