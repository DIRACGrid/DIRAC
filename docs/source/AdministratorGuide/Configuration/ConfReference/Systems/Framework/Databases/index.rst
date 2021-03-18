Systems / Framework / <INSTANCE> / Databases - Sub-subsection
===============================================================

Databases used by Framework System. Note that each database is a separate subsection.

+--------------------------------+----------------------------------------------+----------------------+
| **Name**                       | **Description**                              | **Example**          |
+--------------------------------+----------------------------------------------+----------------------+
| *<DATABASE_NAME>*              | Subsection. Database name                    | ProxyDB              |
+--------------------------------+----------------------------------------------+----------------------+
| *<DATABASE_NAME>/DBName*       | Database name                                | DBName = ProxyDB     |
+--------------------------------+----------------------------------------------+----------------------+
| *<DATABASE_NAME>/Host*         | Database host server where the DB is located | Host = db01.in2p3.fr |
+--------------------------------+----------------------------------------------+----------------------+
| *<DATABASE_NAME>/MaxQueueSize* | Maximum number of simultaneous queries to    | MaxQueueSize = 10    |
|                                | the DB per instance of the client            |                      |
+--------------------------------+----------------------------------------------+----------------------+

The databases associated to Framework System are:
- ComponentMonitoringDB
- NotificationDB
- ProxyDB
- SystemLoggingDB
- UserProfileDB