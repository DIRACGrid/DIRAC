.. _multiProcessorJobs:

===================
MultiProcessor Jobs
===================

MultiProcessor (MP) jobs are a typical case of a type of jobs for which a complex matching is normally requested.
There are several possible use cases. Starting from the case of the resource providers:

- computing resource providers may give their users the possibility to run on their resources only single processor jobs
- computing resource providers may give their users the possibility to run on their resources only multi processor jobs
- computing resource providers may give their users the possibility to run both single and multi processor jobs
- computing resource providers may ask their users to distinguish clearly between single and multi processor jobs
- computing resource providers may need to know the exact number of processors a job is requesting

The configuration of the Computing Elements and the Job Queues that a computing resource provider expose will determine all the above.
Within DIRAC it's possible to describe CEs and Queues precisely enough to satisfy all use cases above.
It should also be remembered that, independently of DIRAC capabilities to accommodate all the cases above, normally,
for a correct resource provisioning and accounting, computing resource providers don't allow multi processor payloads to run on single processor queues.
And, sometimes they also don't allow single processor payloads to run on multi processor queues.

At the same time, from a users' perspective:

- certain jobs may be able to run only in single multi processor mode
- certain jobs may be able to run only in multi multi processor mode (meaning: need at least 2 processors)
- certain multi processor jobs may need a fixed amount of processors
- certain jobs may be able to run both in single or multi processor mode

Within DIRAC it's possible to describe the jobs precisely enough to satisfy all use cases above.
For a description of how to use the DIRAC Job APIs for the use cases above, please refer to the :ref:`tutorial on Job Management <advancedJobManagement>`.
This page explains how to configure the CEs and Queues for satisfying the use cases above,
starting from the fact that single processor jobs are, normally, the default.

As of today (release v6r20p25) it's possible to use the tags mechanism (described in :ref:`tagsAndJobs`) for marking MultiProcessor jobs and queues (or CEs).
