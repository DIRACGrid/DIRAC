================
Framework System
================

The DIRAC FrameworkSystem contains those components that are used for administering DIRAC installations.
Most of them are an essential part of a server installation of DIRAC.

The functionalities that are exposed by the framework system include, but are not limited to,
the Instantiation of DIRAC components, but also the DIRAC commands (scripts),
the management and monitoring of components. 

The management of DIRAC components include their installation and un-installation (the system will keep a history of them)
and a monitoring system that accounts for CPU and memory usage, queries served, used threads, and other parameters. 

Another very important functionality provided by the framework system is proxies management,
via the ProxyManager service and database.

ComponentMonitoring, SecurityLogging, and ProxyManager services are only part of the services that constitute the
Framework of DIRAC.

The following sections add some details for some of the Framework systems.

.. toctree::
   :maxdepth: 1

   ComponentMonitoring/index
   Monitoring/index
   Notification/index
   ProxyManager/index
