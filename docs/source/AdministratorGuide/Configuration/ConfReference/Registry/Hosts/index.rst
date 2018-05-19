Registry / Hosts - Subsections
==============================

In this section each trusted hosts (DIRAC secondary servers) are described using simple attributes.

A subsection called as DIRAC host name must be created and inside of this the following attributes
must be included:

+--------------------------------+------------------------------------------+-----------------------------------------------------------+
| **Name**                       | **Description**                          | **Example**                                               |
+--------------------------------+------------------------------------------+-----------------------------------------------------------+
| *<DIRAC_HOST_NAME>*            | Subsection DIRAC host name               | host-dirac.in2p3.fr                                       |
+--------------------------------+------------------------------------------+-----------------------------------------------------------+
| *<DIRAC_HOST_NAME>/DN*         | Host distinguish name obtained from host | DN = /O=GRID-FR/C=FR/O=CNRS/OU=CC-IN2P3/CN=dirac.in2p3.fr |
|                                | certificate                              |                                                           |
+--------------------------------+------------------------------------------+-----------------------------------------------------------+
| *<DIRAC_HOST_NAME>/Properties* | Properties associated with the host      | Properties = JobAdministrator                             |
|                                |                                          | Properties += FullDelegation                              |
|                                |                                          | Properties += Operator                                    |
|                                |                                          | Properties += CSAdministrator                             |
|                                |                                          | Properties += ProductionManagement                        |
|                                |                                          | Properties += AlarmsManagement                            |
|                                |                                          | Properties += ProxyManagement                             |
|                                |                                          | Properties += TrustedHost                                 |
+--------------------------------+------------------------------------------+-----------------------------------------------------------+
