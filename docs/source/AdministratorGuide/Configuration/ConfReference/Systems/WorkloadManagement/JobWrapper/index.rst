.. _cs-JobWrapper:

Systems / WorkloadManagement / <INSTANCE> / JobWrapper - Sub-subsection
============================================================================

The Job Wrapper Class is instantiated with arguments tailored for running
a particular job. The JobWrapper starts a thread for execution of the job
and a Watchdog Agent that can monitor progress.

The options used to configure JobWrapper are showed in the table below:

+----------------------+-------------------------------------------------+------------------------------+
| **Name**             | **Description**                                 | **Example**                  |
+----------------------+-------------------------------------------------+------------------------------+
| *BufferLimit*        | Size limit of the buffer used for transmission  | BufferLimit = 10485760       |
|                      | between the WN and DIRAC server                 |                              |
+----------------------+-------------------------------------------------+------------------------------+
| *CleanUpFlag*        | Boolean                                         | CleanUpFlag = True           |
+----------------------+-------------------------------------------------+------------------------------+
| *DefaultCatalog*     | Default catalog where must be registered the    | DefaultCatalog = FileCatalog |
|                      | output files if this is not defined by the user |                              |
|                      | FileCatalog define DIRAC file catalog           |                              |
+----------------------+-------------------------------------------------+------------------------------+
| *DefaultCPUTime*     | Default CPUTime expressed in seconds            | DefaultCPUTime = 600         |
+----------------------+-------------------------------------------------+------------------------------+
| *DefaultErrorFile*   | Name of default error file                      | DefaultErrorFile = std.err   |
+----------------------+-------------------------------------------------+------------------------------+
| *DefaultOutputFile*  | Name of default output file                     | DefaultOutputFile = std.out  |
+----------------------+-------------------------------------------------+------------------------------+
| *DefaultOutputSE*    | Default output storage element                  | DefaultOutputSE = IN2P3-disk |
+----------------------+-------------------------------------------------+------------------------------+
| *MaxJobPeekLines*    | Maximum number of output job lines showed       | MaxJobPeekLines = 20         |
+----------------------+-------------------------------------------------+------------------------------+
| *OutputSandboxLimit* | Limit of sandbox output expressed in MB         | OutputSandboxLimit = 10      |
+----------------------+-------------------------------------------------+------------------------------+
| *StopMargin*         | Wall-clock seconds at the end of the batch      | StopMargin = 300             |
|                      | slot reserved for uploading the outputs and     |                              |
|                      | the logs. Deducted once, by the agent that      |                              |
|                      | publishes the slot budget, so consumers of      |                              |
|                      | /LocalSite/CPUTimeLeft must not deduct it       |                              |
|                      | again                                           |                              |
+----------------------+-------------------------------------------------+------------------------------+

Graceful stop
-------------

A payload killed when its slot runs out loses whatever it had produced but not yet
written. An application that knows how to wind down can be signalled instead, early
enough to finish its current unit of work and write its output; the Watchdog stops it
anyway once the budget is spent, so these only buy it the chance.

The same four options may be set per job in the JDL, under the same names, which takes
precedence. Setting them here is what reaches work already submitted, whose JDL is fixed.

+------------------------+-------------------------------------------------+------------------------------+
| **Name**               | **Description**                                 | **Example**                  |
+------------------------+-------------------------------------------------+------------------------------+
| *StopSigRegex*         | Regular expression matched against the          | StopSigRegex = Gauss         |
|                        | command line of the payload processes,          |                              |
|                        | naming the application to signal. Unset         |                              |
|                        | means the mechanism is off                      |                              |
+------------------------+-------------------------------------------------+------------------------------+
| *StopSigNumber*        | Signal to send to the matching processes.       | StopSigNumber = 10           |
|                        | Default 2 (SIGINT)                              |                              |
+------------------------+-------------------------------------------------+------------------------------+
| *StopSigStartWork*     | How much work a payload must have done          | StopSigStartWork = 8370      |
|                        | before it is worth interrupting: below it       |                              |
|                        | there is nothing to save. Defaults to           |                              |
|                        | StopSigFinishWork, so a payload is never        |                              |
|                        | stopped having done less work than stopping     |                              |
|                        | it costs                                        |                              |
+------------------------+-------------------------------------------------+------------------------------+
| *StopSigFinishWork*    | How much computation the application needs      | StopSigFinishWork = 16740    |
|                        | to wind down: finish the unit of work in        |                              |
|                        | progress, close and write its output. In        |                              |
|                        | CPU work rather than seconds, so that one       |                              |
|                        | figure means the same on a fast and a slow      |                              |
|                        | node; divided by this node's                    |                              |
|                        | CPUNormalizationFactor before use. The          |                              |
|                        | graceful stop does nothing until it is set      |                              |
+------------------------+-------------------------------------------------+------------------------------+
