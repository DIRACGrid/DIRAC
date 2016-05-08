Systems / WorkloadManagement / <INSTANCE> / Agents / JobCleaningAgent - Sub-subsection
======================================================================================

The Job Cleaning Agent controls removing jobs from the WMS in the end of their life cycle. The attributes are showed in the following table.

+--------------------+----------------------------------------+---------------------------------------+
| **Name**           | **Description**                        | **Example**                           |
+--------------------+----------------------------------------+---------------------------------------+
| *JobByJob*         | Boolean than express if job by job     | JobByJob = True                       |
|                    | must be processed                      |                                       |
+--------------------+----------------------------------------+---------------------------------------+
| *MaxJobsAtOnce*    | Maximum number of jobs to be processed | MaxJobsAtOnce = 200                   |
|                    | at the same time                       |                                       |
+--------------------+----------------------------------------+---------------------------------------+
| *ProductionTypes*  | Production types                       | ProductionTypes  = DataReconstruction |
|                    |                                        | ProductionTypes += DataStripping      |
|                    |                                        | ProductionTypes += MCSimulation       |
|                    |                                        | ProductionTypes += Merge              |
|                    |                                        | ProductionTypes += production         |
+--------------------+----------------------------------------+---------------------------------------+
| *ThrottlingPeriod* |                                        | ThrottlingPeriod = 0                  |
+--------------------+----------------------------------------+---------------------------------------+

And also the common options for all the agents.