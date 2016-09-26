Systems / WorkloadManagement / <INSTANCE> / Agents / JobCleaningAgent - Sub-subsection
======================================================================================

The Job Cleaning Agent controls removing jobs from the WMS in the end of their life cycle. The attributes are showed in the following table.

+-----------------------------+----------------------------------------+--------------------------------------------+
| **Name**                    | **Description**                        | **Example**                                |
+-----------------------------+----------------------------------------+--------------------------------------------+
| *JobByJob*                  | Boolean: If True jobs are deleted      | | JobByJob = True                          |
|                             | individually not in batches            | | (default is False)                       |
+-----------------------------+----------------------------------------+--------------------------------------------+
| *MaxJobsAtOnce*             | Int: Maximum number of jobs to be      | | MaxJobsAtOnce = 100                      |
|                             | processed at the same time             | | (default is 500)                         |
+-----------------------------+----------------------------------------+--------------------------------------------+
| *ProductionTypes*           | Production types                       | | ProductionTypes  = DataReconstruction    |
|                             |                                        | | ProductionTypes += DataStripping         |
|                             |                                        | | ProductionTypes += MCSimulation          |
|                             |                                        | | ProductionTypes += Merge                 |
|                             |                                        | | ProductionTypes += production            |
+-----------------------------+----------------------------------------+--------------------------------------------+
| *ThrottlingPeriod*          | Float: Seconds to wait between jobs if | | ThrottlingPeriod = 1.0                   |
|                             | JobByJob is True.                      | | (default is 0.)                          |
+-----------------------------+----------------------------------------+--------------------------------------------+
| *RemoveStatusDelay/Done*    | Int: Number of days after which to     | | RemoveStatusDelay/Done = 14              |
|                             | remove jobs in the Done state.         | | (default is 7)                           |
+-----------------------------+----------------------------------------+--------------------------------------------+
| *RemoveStatusDelay/Killed*  | Int: Number of days after which to     | | RemoveStatusDelay/Killed = 14            |
|                             | remove jobs in the Killed state.       | | (default is 7)                           |
+-----------------------------+----------------------------------------+--------------------------------------------+
| *RemoveStatusDelay/Failed*  | Int: Number of days after which to     | | RemoveStatusDelay/Failed = 14            |
|                             | remove jobs in the Failed state.       | | (default is 7)                           |
+-----------------------------+----------------------------------------+--------------------------------------------+
| *RemoveStatusDelay/Any*     | Int: Number of days after which to     | | RemoveStatusDelay/Any = 60               |
|                             | remove any job, irrespective of state. | | (default is -1, to disable this feature) |
+-----------------------------+----------------------------------------+--------------------------------------------+

And also the common options for all the agents.
