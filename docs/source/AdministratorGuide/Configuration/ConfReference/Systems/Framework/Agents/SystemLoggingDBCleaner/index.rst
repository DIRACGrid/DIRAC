Systems / Framework / <INSTANCE> / Agents / SystemLoggingDBCleaner - Sub-subsection
===================================================================================

SystemLoggingDBCleaner erases records whose messageTime column contains a time older than 'RemoveDate' days,
where 'RemoveDate' is an entry in the Configuration Service section of the agent.


The attributes of this agent are showed in the table below:

+--------------+-------------------------------------------+-----------------+
| **Name**     | **Description**                           | **Example**     |
+--------------+-------------------------------------------+-----------------+
| *RemoveDate* | Each many days the database must be clean | RemoveDate = 30 |
|              | Expressed in days                         |                 |
+--------------+-------------------------------------------+-----------------+
