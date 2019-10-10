Systems / WorkloadManagement / <INSTANCE> / Executors - Sub-subsection
=======================================================================

 In this subsection each executor is described.

+------------+----------------------------------+----------------+
| **Name**   | **Description**                  | **Example**    |
+------------+----------------------------------+----------------+
| *Executor* | Subsection named as the Executor | InputData      |
|            | is called.                       |                |
+------------+----------------------------------+----------------+

Common options for all the executors are described in the table below:

+---------------------+---------------------------------------+------------------------------+
| **Name**            | **Description**                       | **Example**                  |
+---------------------+---------------------------------------+------------------------------+
| *LogLevel*          | Log Level associated to the executor  | LogLevel = DEBUG             |
+---------------------+---------------------------------------+------------------------------+
| *LogBackends*       |                                       | LogBackends = stdout, ...    |
+---------------------+---------------------------------------+------------------------------+
| *Status*            | ????Executor Status, possible values  | Status = Active              |
|                     | Active or Inactive                    |                              |
+---------------------+---------------------------------------+------------------------------+


Executors associated with Configuration System:

.. toctree::
   :maxdepth: 2
   
   InputData/index
   JobPath/index
   JobSanity/index
   JobScheduling/index


