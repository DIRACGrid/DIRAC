Registry - Section
==================

This section allows to register users, hosts and groups in DIRAC way. Also some attributes applicable for all
the configuration are defined.

+---------------------------+-----------------------------------------+--------------------------------------------+
| **Name**                  | **Description**                         | **Example**                                |
+---------------------------+-----------------------------------------+--------------------------------------------+
| *DefaultGroup*            | Default user group to be used           | DefaultGroup = user                        |
+---------------------------+-----------------------------------------+--------------------------------------------+
| *QuarantineGroup*         | Querantine user group is usually        | QuarantineGroup = lowPriority_user         |
|                           | to be used in case you want to set      |                                            |
|                           | users in groups by hand                 |                                            |
|                           | as a "punishment" for a certain period  |                                            |
|                           | of time                                 |                                            |
+---------------------------+-----------------------------------------+--------------------------------------------+
| *DefaultProxyTime*        | Default proxy time expressed in seconds | DefaultProxyTime = 4000                    |
+---------------------------+-----------------------------------------+--------------------------------------------+


.. toctree::
   :maxdepth: 2
   
   Groups/index
   Hosts/index
   Users/index
   VO/index
