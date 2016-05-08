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



