Systems / WorkloadManagement / <INSTANCE> / Executors / JobScheduling - Sub-subsection
=======================================================================================

The Job Scheduling Executor takes the information gained from all previous
optimizers and makes a scheduling decision for the jobs.
Subsequent to this jobs are added into a Task Queue and pilot agents can be submitted.
All issues preventing the successful resolution of a site candidate are discovered
here where all information is available.
This Executor will fail affected jobs meaningfully.

+-------------------------+-----------------------------------------+--------------------------------------------+
| **Name**                | **Description**                         | **Example**                                |
+-------------------------+-----------------------------------------+--------------------------------------------+
| *RescheduleDelays*      | How long to hold job after              | RescheduleDelays=60, 180, 300, 600         |
|                         | rescheduling                            |                                            |
+-------------------------+-----------------------------------------+--------------------------------------------+
| *ExcludedOnHoldJobTypes*| List of job types to exclude from       |                                            |
|                         | holding after rescheduling              |                                            |
+-------------------------+-----------------------------------------+--------------------------------------------+
| *InputDataAgent*        | Name of the InputData executor          | InputDataAgent = InputData                 |
|                         | instance                                |                                            |
+-------------------------+-----------------------------------------+--------------------------------------------+
| *RestrictDataStage*     | Are users restricted from staging       |  RestrictDataStage = False                 |
|                         |                                         |                                            |
+-------------------------+-----------------------------------------+--------------------------------------------+
| *HoldTime*              | How long jobs are held for              | HoldTime = 300                             |
|                         |                                         |                                            |
+-------------------------+-----------------------------------------+--------------------------------------------+
| *StagingStatus*         | Status when staging                     | StagingStatus = Staging                    |
|                         |                                         |                                            |
+-------------------------+-----------------------------------------+--------------------------------------------+
| *StagingMinorStatus*    | Minor status when staging               | StagingMinorStatus = "Request To Be Sent"  |
|                         |                                         |                                            |
+-------------------------+-----------------------------------------+--------------------------------------------+
| *AllowInvalidSites*     | If set to False, jobs will be held if   | AllowInvalidSites = False                  |
|                         | any of the Sites specified are invalid. | (default value is True)                    |
+-------------------------+-----------------------------------------+--------------------------------------------+
| *CheckOnlyTapeSEs*      | If set to False, the optimizer will     | CheckOnlyTapeSEs = False                   |
|                         | check the presence of all replicas      | (default value is True)                    |
+-------------------------+-----------------------------------------+--------------------------------------------+
| *CheckPlatform*         | If set to True, the optimizer will      | CheckPlatform = True                       |
|                         | verify the job JDL Platform setting.    | (default value is False)                   |
+-------------------------+-----------------------------------------+--------------------------------------------+

