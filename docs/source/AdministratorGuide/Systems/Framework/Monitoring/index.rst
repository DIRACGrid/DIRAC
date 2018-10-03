.. _framework_monitoring:

================================
The Framework/monitoring service
================================

The Framework/Monitoring service collects information from all the active DIRAC services and Agents.
The information are collected in *rrd* files which are keeping the monitoring information.
This information is available as time dependent plots via the ActivityMonitor web portal application.
You can access these plots via the "System overview plots" tab in this application. In particular, it shows the load of the services
in terms of CPU/Memory but also numbers of queries served, numbers of active threads, pending queries, etc.
These plots are very useful for understanding of your services behavior, for example, of your FileCatalog service.

The bookkeeping of the rrd files is kept in an sqlite database usually kept in /opt/dirac/data/monitoring/monitoring.db file.
There is no cleaning procedure foreseen for the rrd files.

A Monitoring System based on ElasticSearch database as backend is possible,
please read about it in :ref:`Monitoring <monitoring_system>`. 
