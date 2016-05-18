================================
Workload Management System (WMS)
================================

The DIRAC WMS is a Pilot based Workload Management System.
It provides high user jobs efficiency, hiding the heterogeneity of the the underlying computing resources.

Jobs are not sent directly to the Computing Elements, or to any Computing resource.
Instead, their description and requirements are stored in the DIRAC WMS (in a JDL, Job Description Language).
JDLs are then matched by pilots running on the Worker Nodes.


.. toctree::
   :maxdepth: 1

   Pilots/index
   Jobs/index
   JobPriorities/index
