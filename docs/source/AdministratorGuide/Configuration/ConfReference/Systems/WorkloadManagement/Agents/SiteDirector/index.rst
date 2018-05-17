.. _conf-SiteDirector:

Systems / WorkloadManagement / <INSTANCE> / Agents / SiteDirector - Sub-subsection
==================================================================================

Site director is in charge of submit pilot jobs to special Computing Elements.
 
Special attributes for this agent are (updated for v6r20):
 
+---------------------------------+----------------------------------------+-------------------------------------------------------------------+
| **Name**                        | **Description**                        | **Example**                                                       |
+---------------------------------+----------------------------------------+-------------------------------------------------------------------+
| *CETypes*                       | List of CEs types allowed to submit    | CETypes = CREAM                                                   |
|                                 | pilot jobs (default: 'any')            |                                                                   |
+---------------------------------+----------------------------------------+-------------------------------------------------------------------+
| *CEs*                           | List of CEs where to submit            | CEs = ce01.in2p3.fr                                               |
|                                 | pilot jobs (default: 'any')            |                                                                   |
+---------------------------------+----------------------------------------+-------------------------------------------------------------------+
| *Site*                          | Sites name list where the pilots will  | Site =                                                            |
|                                 | be submitted                           |                                                                   |
+---------------------------------+----------------------------------------+-------------------------------------------------------------------+
| *PilotDN*                       | Pilot DN used to submit the            | PilotDN =  /O=GRID-FR/C=FR/O=CNRS/OU=CPPM/CN=Vanessa Hamar        |
|                                 | pilot jobs (default: 'any')            |                                                                   |
+---------------------------------+----------------------------------------+-------------------------------------------------------------------+
| *PilotGroup*                    | DIRAC group used to submit the pilot   | PilotGroup = dirac_pilot                                          |
|                                 | jobs.                                  |                                                                   |
|                                 | If not found here, what is in          |                                                                   |
|                                 | Operations/Pilot section of the CS     |                                                                   |
|                                 | will be used                           |                                                                   |
+---------------------------------+----------------------------------------+-------------------------------------------------------------------+
| *GetPilotOutput*                | Boolean value used to indicate the     | GetPilotOutput = True                                             |
|                                 | pilot output will be or not retrieved  |                                                                   |
|                                 | (default: False)                       |                                                                   |
+---------------------------------+----------------------------------------+-------------------------------------------------------------------+
| *GridEnv*                       | Path where is located the file to      | GridEnv = /usr/profile.d/grid-env                                 |
|                                 | load Grid Environment Variables        |                                                                   |
+---------------------------------+----------------------------------------+-------------------------------------------------------------------+
| *MaxQueueLength*                | Maximum cputime used for a queue, will | MaxQueueLength = 3x86400                                          |
|                                 | set maxCPU time to this value          |                                                                   |
+---------------------------------+----------------------------------------+-------------------------------------------------------------------+
| *SendPilotAccounting*           | Boolean value than indicates if the    | SendPilotAccounting = yes                                         |
|                                 | pilot job will send information for    |                                                                   |
|                                 | accounting                             |                                                                   |
|                                 | (default: True)                        |                                                                   |
+---------------------------------+----------------------------------------+-------------------------------------------------------------------+
| *UpdatePilotStatus*             | Attribute used to define if the status | UpdatePilotStatus = True                                          |
|                                 | of the pilot will be updated           |                                                                   |
|                                 | (default: True)                        |                                                                   |
+---------------------------------+----------------------------------------+-------------------------------------------------------------------+
| *WorkDirectory*                 | Working Directory in the CE            | WorkDirectory = /tmp                                              |
+---------------------------------+----------------------------------------+-------------------------------------------------------------------+
| *VO* or *Community*             | Optional, will be obtained by other    |                                                                   |
|                                 | means if not set                       |                                                                   |
+---------------------------------+----------------------------------------+-------------------------------------------------------------------+
| *Group*                         | Which group is allowed to use these    |                                                                   |
|                                 | pilots                                 |                                                                   |
+---------------------------------+----------------------------------------+-------------------------------------------------------------------+
| *PilotLogLevel*                 | LogLevel of the pilot                  | PilotLogLevel = DEBUG                                             |
+---------------------------------+----------------------------------------+-------------------------------------------------------------------+
| *MaxJobsInFillMode*             | How many jobs the pilot can run        | MaxJobsInFillMode=5                                               |
+---------------------------------+----------------------------------------+-------------------------------------------------------------------+
| *MaxPilotsToSubmit*             | How many pilots to submit per cycle    |                                                                   |
+---------------------------------+----------------------------------------+-------------------------------------------------------------------+
| *PilotWaitingFlag*              | Boolean to limit the number of waiting | PilotWaitingFlag = False                                          |
|                                 | pilots                                 |                                                                   |
+---------------------------------+----------------------------------------+-------------------------------------------------------------------+
| *MaxPilotWaitingTime*           | How old pilots can be to count them    |                                                                   |
|                                 | as a waiting pilot???                  |                                                                   |
+---------------------------------+----------------------------------------+-------------------------------------------------------------------+
| *FailedQueueCycleFactor*        | How many cylces to skip if queue was   |                                                                   |
|                                 | not working                            |                                                                   |
+---------------------------------+----------------------------------------+-------------------------------------------------------------------+
| *AddPilotsToEmptySites*         | To submit pilots to empty sites        | AddPilotsToEmptySites = True                                      |
|                                 | in any case (False by default)         |                                                                   |
+---------------------------------+----------------------------------------+-------------------------------------------------------------------+
| *PilotStatusUpdateCycleFactor*  | Deafult: 10                            |                                                                   |
+---------------------------------+----------------------------------------+-------------------------------------------------------------------+
| *Pilot3*                        | To submit pilot 3 or not               | Pilot3 = True                                                     |
+---------------------------------+----------------------------------------+-------------------------------------------------------------------+

