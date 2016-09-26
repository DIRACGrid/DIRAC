Systems / WorkloadManagement / <INSTANCE> / Agents / StalledJobAgent - Sub-subsection
======================================================================================

The StalledJobAgent hunts for stalled jobs in the Job database. Jobs in "running"state not receiving a 
heart beat signal for more than stalledTime seconds will be assigned the "Stalled" state.

The FailedTimeHours and StalledTimeHours are actually given in number of cycles. One Cycle is 30 minutes
and can be changed in the Systems/WorkloadManagement/<Instance>/JobWrapper section with the CheckingTime
and MinCheckingTime options


+--------------------+------------------------------------------+------------------------------+
| **Name**           | **Description**                          | **Example**                  |
+--------------------+------------------------------------------+------------------------------+
| *FailedTimeHours*  | How much time in hours pass before a     | FailedTimeHours = 6          |
|                    | stalled job is declared as failed        |                              |
|                    | Note: Not actually in hours              |                              |
+--------------------+------------------------------------------+------------------------------+
| *StalledTimeHours* | How much time in hours pass before       | StalledTimeHours = 2         |
|                    | running job is declared as stalled       |                              |
|                    | Note: Not actually in hours              |                              |
+--------------------+------------------------------------------+------------------------------+
| *MatchedTime*      | Age in seconds until matched jobs are    | MatchedTime = 7200           |
|                    | rescheduled                              |                              |
|                    |                                          |                              |
+--------------------+------------------------------------------+------------------------------+
| *RescheduledTime*  | Age in seconds until rescheduled jobs    | RescheduledTime = 600        |
|                    | are rescheduled                          |                              |
|                    |                                          |                              |
+--------------------+------------------------------------------+------------------------------+
| *CompletedTime*    | Age in seconds until completed jobs      | CompletedTime = 86400        |
|                    | are declared failed, unless their minor  |                              |
|                    | status is "Pending Requests"             |                              |
+--------------------+------------------------------------------+------------------------------+
