.. _JobsMatching:

==============================================
Matching WNs capabilities to Jobs requirements
==============================================

Pilots determine the WNs capabilities and the JobAgent started by the pilot will contact the Matcher service to match a job, selected from the TaskQueueDB.

<to expand with an example>

Capabilities and requirements include but are not limited to:

* *destination*: a (list of) site name(s)
* *CPUTime*: the (estimated) time, expressed in HS06s
* *platform*: the platform of the WN (which is determined by its OS, and not only), also refer to :ref:`resourcesComputing`
* *generic tags*: read about it in further sections
