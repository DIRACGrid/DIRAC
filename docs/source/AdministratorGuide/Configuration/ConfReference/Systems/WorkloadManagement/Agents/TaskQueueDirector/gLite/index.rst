Systems / WorkloadManagement / <INSTANCE> / Agents / TaskQueueDirector / gLite - Sub-subsection
===============================================================================================

Options available to configure gLite pool submission are showed in a table below:

+------------------------+----------------------------------------------+---------------------------------------------------------------------------------+
| **Name**               | **Description**                              | **Example**                                                                     |
+------------------------+----------------------------------------------+---------------------------------------------------------------------------------+
| *Failing*              |                                              | Failing =                                                                       |
+------------------------+----------------------------------------------+---------------------------------------------------------------------------------+
| *GenericPilotDN*       | Distinguish name to be used to submit the    | GenericPilotDN = /O=GRID-FR/C=FR/O=CNRS/OU=CPPM/CN=Vanessa Hamar                |
|                        | pilot jobs                                   |                                                                                 |
+------------------------+----------------------------------------------+---------------------------------------------------------------------------------+
| *GenericPilotGroup*    | DIRAC group used to submit the pilot jobs    | GenericPilotGroup = dirac_pilot                                                 |
+------------------------+----------------------------------------------+---------------------------------------------------------------------------------+
| *GridMiddleware*       | Pool submission of Grid middleware           | GridMiddleware = gLite                                                          |
+------------------------+----------------------------------------------+---------------------------------------------------------------------------------+
| *LoggingServers*       | Loggin servers available for the pool        | LoggingServers = lb01.in2p3.fr                                                  |
+------------------------+----------------------------------------------+---------------------------------------------------------------------------------+
| *MaxJobsinFillMode*    | Maximum number of jobs to run by a pilot job | MaxJobsinFillMode = 5                                                           |
+------------------------+----------------------------------------------+---------------------------------------------------------------------------------+
| *PrivatePilotFraction* | Portion of private pilots to be submitted    | PrivatePilotFraction = 0.5                                                      |
|                        | expressed in a valor between 0 and 1         |                                                                                 |
+------------------------+----------------------------------------------+---------------------------------------------------------------------------------+
| *Rank*                 | Rank in gLite format                         | Rank = ( other.GlueCEStateWaitingJobs == 0 ? ( other.GlueCEStateFreeCPUs * 10 / |
|                        |                                              | other.GlueCEInfoTotalCPUs + other.GlueCEInfoTotalCPUs / 500 ) :                 |
|                        |                                              | -other.GlueCEStateWaitingJobs * 4 / (other.GlueCEStateRunningJobs + 1 ) - 1 )   |
+------------------------+----------------------------------------------+---------------------------------------------------------------------------------+
| *ResourceBrokers*      | List of Grid Resource Brokers available      | ResourceBrokers = rb01.in2p3.fr                                                 |
+------------------------+----------------------------------------------+---------------------------------------------------------------------------------+
