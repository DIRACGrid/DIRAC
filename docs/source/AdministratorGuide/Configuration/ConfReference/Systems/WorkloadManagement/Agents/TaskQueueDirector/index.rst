Systems / WorkloadManagement / <INSTANCE> / Agents / TaskQueueDirector - Sub-subsection
=======================================================================================

The TaskQueue Director Agent controls the submission of pilots via the PilotDirectors. 
These are Backend-specific PilotDirector derived classes. This is a simple wrapper that performs the instantiation 
and monitoring of the PilotDirector instances and add workload to them via ThreadPool mechanism.

From the base Agent class it uses the following configuration Parameters
       - WorkDir:
       - PollingTime:
       - MaxCycles:

The following parameters are searched for in WorkloadManagement/TaskQueueDirector:
       - ThreadStartDelay:
       - SubmitPools: All the Submit pools that are to be initialized
       - DefaultSubmitPools: If no specific pool is requested, use these

It will use those Directors to submit pilots for each of the Supported SubmitPools
       - SubmitPools (see above)


SubmitPools may refer to:
       - a full GRID infrastructure (like EGEE, OSG, NDG,...) access remotely through RBs or WMSs servers distributing the load over all available resources (CEs) using ad hoc middleware (gLite, LCG, ...).
       - individual GRID Computing Elements again access remotely through their corresponding GRID interface using ad hoc middleware.
       - classic batch systems (like LSF, BQS, PBS, Torque, Condor, ...) access locally trough their corresponding head nodes using their onw specific tools
       - standalone computers access by direct execution (fork or exec)

In first two cases, the middleware takes care of properly handling the secure transfer of the
payload to the executing node. In the last two DIRAC will take care of all relevant security
aspects.

For every SubmitPool category (GRID or DIRAC) and there must be a corresponding Section with the
necessary parameters:

- Pool: if a dedicated Threadpool is desired for this SubmitPool

GRID:
       - GridMiddleware: <GridMiddleware>PilotDirector module from the PilotAgent directory will be used, currently LCG, gLite types are supported

     For every supported GridMiddleware there must be a corresponding Section with the necessary parameters:
       - gLite:

       - LCG:

       - DIRAC:

For every supported "Local backend" there must be a corresponding Section with the necessary parameters:
       - PBS:

       - Torque:

       - LSF:

       - BQS:

       - Condor:

(This are the parameters referring to the corresponding SubmitPool and PilotDirector classes,
not the ones referring to the CE object that does the actual submission to the backend)

The following parameters are taken from the TaskQueueDirector section if not
present in the corresponding SubmitPool section:

       - GenericPilotDN:
       - GenericPilotGroup:


The pilot submission logic is as follows:

        - Determine prioritySum: sum of the Priorities for all TaskQueues in the system.

        - Determine pilotsPerPriority: result of dividing the  number of pilots to submit
          per iteration by the prioritySum.

        - select TaskQueues from the WMS system appropriated for PilotSubmission by the supported
          SubmitPools

        - For each TaskQueue determine a target number of pilots to submit:

          - Multiply the priority by pilotsPerPriority.
          - Apply a correction factor for proportional to maxCPU divided by CPU of the
            TaskQueue ( double number of pilots will be submitted for a TaskQueue with
            half CPU required ). To apply this correction the minimum CPU considered is
            lowestCPUBoost.
          - Apply poisson statistics to determine the target number of pilots to submit
            (even a TQ with a very small priorities will get a chance of getting
            pilots submitted).
          - Determine a maximum number of "Waiting" pilots in the system:
            ( 1 + extraPilotFraction ) * [No. of Jobs in TaskQueue] + extraPilots
          - Attempt to submit as many pilots a the minimum between both number.
          - Pilot submission request is inserted into a ThreadPool.

        - Report the sum of the Target number of pilots to be submitted.

        - Wait until the ThreadPool is empty.

        - Report the actual number of pilots submitted.

In summary:

All TaskQueues are considered on every iteration, pilots are submitted
statistically proportional to the priority and the Number of waiting tasks
of the TaskQueue, boosted for the TaskQueues with lower CPU requirements and
limited by the difference between the number of waiting jobs and the number of
already waiting pilots.


This module is prepared to work:

       - locally to the WMS DIRAC server and connect directly to the necessary DBs.
       - remotely to the WMS DIRAC server and connect via appropriated DISET methods.

Obsolete Job JDL Option:

        GridExecutable
        SoftwareTag


+------------------------+--------------------------------------------------+---------------------------------------------------------------------------------------+
| **Name**               | **Description**                                  | **Example**                                                                           |
+------------------------+--------------------------------------------------+---------------------------------------------------------------------------------------+
| *AllowedSubmitPools*   | Pools where is possible to submit pilot jobs     | AllowedSubmitPools = gLite                                                            |
|                        |                                                  | AllowedSubmitPools += DIRAC                                                           |
+------------------------+--------------------------------------------------+---------------------------------------------------------------------------------------+
| *DefaultSubmitPools*   | Default submit pilot pools                       | DefaultSubmitPools = DIRAC                                                            |
+------------------------+--------------------------------------------------+---------------------------------------------------------------------------------------+
| *extraPilots*          | Number of extra pilot jobs to be submitted       | extraPilots = 4                                                                       |
+------------------------+--------------------------------------------------+---------------------------------------------------------------------------------------+
| *extraPilotFraction*   | Percentage of private pilots fraction to be      | extraPilotFraction = 0.2                                                              |
|                        | submitted                                        |                                                                                       |
+------------------------+--------------------------------------------------+---------------------------------------------------------------------------------------+
| *ExtraPilotOptions*    | Extra configuration options to be added during   | ExtraPilotOptions = -g 2010-11-20                                                     |
|                        | pilot jobs are executed                          |                                                                                       |
+------------------------+--------------------------------------------------+---------------------------------------------------------------------------------------+
| *GridMiddleware*       | Pool Grid middleware                             | GridMiddleware = gLite                                                                |
+------------------------+--------------------------------------------------+---------------------------------------------------------------------------------------+
| *ListMatchDelay*       |                                                  | ListMatchDelay =                                                                      |
+------------------------+--------------------------------------------------+---------------------------------------------------------------------------------------+
| *lowestCPUBoost*       |                                                  | lowestCPUBoost = 7200                                                                 |
+------------------------+--------------------------------------------------+---------------------------------------------------------------------------------------+
| *maxPilotWaitingHours* | Maximum number hours of pilots in waiting status | maxPilotWaitingHours = 6                                                              |
+------------------------+--------------------------------------------------+---------------------------------------------------------------------------------------+
| *maxThreadsInPool*     | Maximum number of threads by pool                | maxThreadsInPool = 2                                                                  |
+------------------------+--------------------------------------------------+---------------------------------------------------------------------------------------+
| *minThreadsInPool*     | Minimum number of threads by pool                | minThreadsInPool = 0                                                                  |
+------------------------+--------------------------------------------------+---------------------------------------------------------------------------------------+
| *pilotsPerIteration*   | Number of pilots by iteration                    | pilotsPerIteration = 40                                                               |
+------------------------+--------------------------------------------------+---------------------------------------------------------------------------------------+
| *PilotScript*          | Path in DIRAC server where the pilot script is   | PilotScript = /opt/dirac/pro/DIRAC/WorkloadManagementSystem/PilotAgent/dirac-pilot.py |
|                        | located                                          |                                                                                       |
+------------------------+--------------------------------------------------+---------------------------------------------------------------------------------------+
| *SubmitPools*          | Pools where is possible to submit pilot jobs     | SubmitPools = gLite                                                                   |
+------------------------+--------------------------------------------------+---------------------------------------------------------------------------------------+
| *ThreadStartDelay*     | ThreadStartDelay                                 | ThreadStartDelay = 0                                                                  |
+------------------------+--------------------------------------------------+---------------------------------------------------------------------------------------+
| *totalThreadsInPool*   | Total number of threads for each pool            | totalThreadsInPool = 40                                                               |
+------------------------+--------------------------------------------------+---------------------------------------------------------------------------------------+

Submission pools:

.. toctree::
   :maxdepth: 2
   
   gLite/index
   DIRAC/index
