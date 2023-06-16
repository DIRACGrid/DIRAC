.. contents:: Table of contents
   :depth: 3

===========================
Workload Management System
===========================

--------
Overview
--------

The system provides high user jobs efficiency, hiding the heterogeneity of the underlying computing resources.

It realizes the task scheduling paradigm with Generic Pilot Jobs.
This task scheduling method solves many problems of using unstable distributed computing resources which are available in computing grids.

----------------
DIRAC JobWrapper
----------------

The JobAgent is creating a file that is made from the JobWrapperTemplate.py file.
It creates a temporary file using this file as a template, which becomes Wrapper_<jobID> somewhere in the workDirectory.
It is this file that is then submitted as a real "job wrapper script".

The JobWrapper is not a job wrapper, but is an object that is used by the job wrapper
(i.e. the JobWrapperTemplate’s execute() method) to actually do the work.

The only change made in the "template" file is the following:
wrapperTemplate = wrapperTemplate.replace( "@SITEPYTHON@", str( siteRoot ) )

Then the file is submitted in bash using the defined CE (the InProcessCE in the default case)

The sequence executed is ("job" is the JobWrapper object here ;-) ):

.. code-block:: python

   job.initialize( arguments )
   #[…]
   result = job.transferInputSandbox( arguments['Job']['InputSandbox'] )
   #[…]
   result = job.resolveInputData()
   #[…]
   result = job.execute( arguments )
   #[…]
   result = job.processJobOutputs( arguments )
   #[…]
   return job.finalize( arguments )

The watchdog is started in job.execute().
A direct consequence is that the time taken to download the input files is not taken into account for the WallClock time.

A race condition might happen inside this method.
The problem here is that we submit the process in detached mode (or in a thread, not clear as here thread may be used for process),
wait 10 seconds and expect it to be started.
If this fails, the JobWrapperTemplate gives up, but if however the detached process runs, it continues executing as if nothing happened!
It is there that there is the famous gJobReport.setJobStatus( 'Failed', 'Exception During Execution', sendFlag = False )
which is sometimes causing jobs to go to "Failed" and then continue.

There is a nice "feature" of this complex cascade which is that the jobAgent reports "Job submitted as ..."
(meaning the job was submitted to the local CE, i.e. the InProcessCE in our case) _after_ the "job" is actually executed!!!

The JobWrapper can also interpret error codes from the application itself.
An error code is, for example, the DErrno.WMSRESC (1502) error code, which will instruct the JobWrapperTemplate to reschedule
the current job.


-------------------
Server Architecture
-------------------

It is based on layered architecture and is based on DIRAC architecture:

* **Services**

  * JobManagerHandler
  * JobMonitoringHandler
  * JobPolicy
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

  * ElasticJobParametersDB
    ElasticJobParametersDB class is a front-end to the Elastic/OpenSearch based index providing Job Parameters.
    It is used in most of the WMS components and is based on Elastic/OpenSearch.
