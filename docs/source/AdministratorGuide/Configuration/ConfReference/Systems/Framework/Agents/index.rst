Systems / Framework / <INSTANCE> / Agents - Sub-subsection
==============================================================

 In this subsection each agent is described.

+----------+----------------------------------+---------------+
| **Name** | **Description**                  | **Example**   |
+----------+----------------------------------+---------------+
| *Agent*  | Subsection named as the agent is | CAUpdateAgent |
|          | called.                          |               |
+----------+----------------------------------+---------------+

Common options for all the agents:

+---------------------+---------------------------------------+------------------------------+
| **Name**            | **Description**                       | **Example**                  |
+---------------------+---------------------------------------+------------------------------+
| *LogLevel*          | Log Level associated to the agent     | LogLevel = DEBUG             |
+---------------------+---------------------------------------+------------------------------+
| *LogBackends*       |                                       | LogBackends = stdout, ...    |
+---------------------+---------------------------------------+------------------------------+
| *MaxCycles*         | Maximum number of cycles made for     | MaxCycles = 500              |
|                     | Agent                                 |                              |
+---------------------+---------------------------------------+------------------------------+
| *MonitoringEnabled* | Indicates if the monitoring of agent  | MonitoringEnabled = True     |
|                     | is enabled. Boolean values            |                              |
+---------------------+---------------------------------------+------------------------------+
| *PollingTime*       | Each many time a new cycle must start | PollingTime = 2600           |
|                     | expresed in seconds                   |                              |
+---------------------+---------------------------------------+------------------------------+
| *Status*            | Agent Status, possible values Active  | Status = Active              |
|                     | or Inactive                           |                              |
+---------------------+---------------------------------------+------------------------------+


Agents associated with Framework System:

.. toctree::
   :maxdepth: 2

   CAUpdateAgent/index
   MyProxyRenewalAgent/index
