Systems / WorkloadManagement / <INSTANCE> / Executors / JobPath - Sub-subsection
====================================================================================
The Job Path Agent determines the chain of Optimizing Agents that must
work on the job prior to the scheduling decision.

Initially this takes jobs in the received state and starts the jobs on the
optimizer chain.  The next development will be to explicitly specify the
path through the optimizers.


+---------------------+---------------------------------------+--------------------------------------------+
| **Name**            | **Description**                       | **Example**                                |
+---------------------+---------------------------------------+--------------------------------------------+
| *BasePath*          | Path for jobs through the executors   | BasePath = JobPath, JobSanity              |
|                     |                                       |                                            |
+---------------------+---------------------------------------+--------------------------------------------+
| *VOPlugin*          | Name of a VO Plugin???                | VOPlugin = ''                              |
|                     |                                       |                                            |
+---------------------+---------------------------------------+--------------------------------------------+
| *InputData*         | Name of the InputData instance        | InputData = InputData                      |
|                     |                                       |                                            |
+---------------------+---------------------------------------+--------------------------------------------+
| *EndPath*           | Last executor for a job               | EndPath = JobScheduling                    |
|                     |                                       |                                            |
+---------------------+---------------------------------------+--------------------------------------------+
