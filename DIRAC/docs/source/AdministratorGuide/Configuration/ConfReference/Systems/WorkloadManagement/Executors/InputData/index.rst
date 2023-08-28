Systems / WorkloadManagement / <INSTANCE> / Executors / InputData - Sub-subsection
====================================================================================

The Input Data Executor queries the file catalog for specified job input data and adds the
relevant information to the job optimizer parameters to be used during the
scheduling decision.

+---------------------+---------------------------------------+----------------------------------------------+
| **Name**            | **Description**                       | **Example**                                  |
+---------------------+---------------------------------------+----------------------------------------------+
| *FailedJobStatus*   | MinorStatus if Executor fails the job | FailedJobStatus = "Input Data Not Available" |
|                     |                                       |                                              |
+---------------------+---------------------------------------+----------------------------------------------+
| *CheckFileMetadata* | Boolean, check file metadata;         | CheckFileMetadata = True                     |
|                     | will ignore Failover SE files         |                                              |
+---------------------+---------------------------------------+----------------------------------------------+
