.. _requestManagementSystem:

=========================
Request Management System
=========================


The DIRAC Request Management System (RMS) is a very generic system that allows for asynchronous actions execution. Its application ranges from failover system (if a DIRAC service or a StorageElement is unavailable at a certain point in time) to asynchronous task list (typically, for large scale data management operations like replications or removals). The RMS service is itself resilient to failure thanks to Request Proxies that can be scattered around your installation.

In order to have the an RMS system working, please see :ref:`rmsComponents`


.. toctree::
   :maxdepth: 1

   concepts
   rmsObjects
   rmsComponents
