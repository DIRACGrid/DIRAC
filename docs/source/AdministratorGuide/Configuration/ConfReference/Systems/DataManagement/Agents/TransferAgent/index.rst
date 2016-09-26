Systems / DataManagement / <SETUP> / Agent / TransferAgent - Sub-subsection
===========================================================================

The TransferAgent is an agent processing *transfer* Requests. Special options are required to configure this agent, 
depending of availablility of FTS system.

+-------------------------+---------------------------------------------------+------------------------------------------+
| **Name**                | **Description**                                   | **Example and default value**            |
+=========================+===================================================+==========================================+
| *RequestsPerCycle*      | Number of requests executed in one agent's cycle  | RequestsPerCycle = 10                    |
+-------------------------+---------------------------------------------------+------------------------------------------+
| *MinProcess*            | Minimal number of sub-processes running.          | MinProcess = 1                           |
+-------------------------+---------------------------------------------------+------------------------------------------+
| *MaxProcess*            | Maximal number of sub-processes running.          | MaxProcess = 4                           |
+-------------------------+---------------------------------------------------+------------------------------------------+
| *ProcessPoolQueueSize*  | Size of queues used by ProcessPool.               | ProcessPoolQueueSize = 10                |
+-------------------------+---------------------------------------------------+------------------------------------------+
| *RequestType*           | Request type.                                     | RequestType = transfer                   |
+-------------------------+---------------------------------------------------+------------------------------------------+
| *shifterProxy*          | Proxy to use.                                     | shifterProxy = DataManager               |
+-------------------------+---------------------------------------------------+------------------------------------------+
| *TaskMode*              | Flag to enable/disable tasks execution.           | TaskMode = True                          |
+-------------------------+---------------------------------------------------+------------------------------------------+
| *FTSMode*               | Flag to enable/disable FTS scheduling.            | FTSMode = False                          |
+-------------------------+---------------------------------------------------+------------------------------------------+
| *ThroughputTimescale*   | Monitoring time period of the FTS processing used | ThroughputTimescale = 3600               |
|                         | for scheduling (in seconds).                      |                                          |
+-------------------------+---------------------------------------------------+------------------------------------------+
| *HopSigma*              | Acceptable time shift to start of FTS transfer.   | HopSigme = 0.0                           |
+-------------------------+---------------------------------------------------+------------------------------------------+
| *SchedulingType*        | Choose transfer speed between number of files per | SchedulingType = Files                   |
|                         | hour or amount of transfered data per hour.       |                                          |
+-------------------------+---------------------------------------------------+------------------------------------------+
| *ActiveStrategies*      | List of active startegies to use.                 | ActiveStrategies = MinimiseTotalWait     |
+-------------------------+---------------------------------------------------+------------------------------------------+
| *AcceptableFailureRate* | Percentage limit of success rate in monitored FTS | AcceptableFailureRate = 75               |
|                         | transfers to accept/reject FTS channel.           |                                          |
+-------------------------+---------------------------------------------------+------------------------------------------+

By default TransferAgent is running in both modes (task execution and FTS scheduling), but for online processing 
*FTSMode* should be disabled.
