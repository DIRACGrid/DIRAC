Systems / WorkloadManagement / <INSTANCE> / Service / Matcher - Sub-subsection
==============================================================================

Matcher class. It matches Agent Site capabilities to job requirements.
It also provides an XMLRPC interface to the Matcher

A special authorization needs to be added:

+-----------------------+----------------------------------------------+-----------------------------------+
| **Name**              | **Description**                              | **Example**                       |
+-----------------------+----------------------------------------------+-----------------------------------+
| *getActiveTaskQueues* | Define DIRAC group allowed to get the active | getActiveTaskQueues = dirac_admin |
|                       | task queues in the system                    |                                   |
+-----------------------+----------------------------------------------+-----------------------------------+
