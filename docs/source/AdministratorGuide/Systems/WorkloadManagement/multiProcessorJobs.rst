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
For a description of how to use the DIRAC Job APIs for the use cases above, please refer to the :ref:`tutorial on Job Management <advancedJobManagemnt>`.
This page explains how to configure the CEs and Queues for satisfying the use cases above,
starting from the fact that single processor jobs are, normally, the default.

DIRAC provides a generic mechanism for matching computing capabilities with resource providers, and this is done using generic "Tags".
Tags can be used by the users to "mark (tag)" their jobs with requirements, and should be used by DIRAC admins to identify CEs or Queues.

So, as always it's a matter of what's written in the CS:
* Meaning that a CE or a Queue has **Tag=X** means that it's *capable() of running jobs that *require* **Tag=X**.
* Meaning that a CE or a Queue has **RequiredTag=X** means that it will *accept only* jobs that *require* **Tag=X**.


Let's take an example::

      DIRAC.MySite.org
      {
        Name = Test
        CEs
        {
          CE.MySite.org
          {
            CEType = Test
            Queues
            {
              # This queue exposes no Tags. So it will accept (match) all jobs that require no tags
              noTagsQueue
              {
                # the following fields are not important
                SI00 = 2400
                maxCPUTime = 200
                MaxTotalJobs = 5
                MaxWaitingJobs = 10
                BundleProxy = True
                RemoveOutput = True
              }
              # This queue has Tag = MultiProcessor. So it will accept:
              # - jobs that require Tag = MultiProcessor (and no others)
              # - jobs that require no Tags
              MPTagQueue
              {
                Tag = MultiProcessor
                ...
              }
              # This queue has Tag = [MultiProcessor, 8Processors]. So it will accept:
              # - jobs that require both the tags above (and no others)
              # - jobs that require Tag = MultiProcessor (and no others)
              # - jobs that require Tag = 8Processors (and no others)
              # - jobs that require no Tags
              MultipleMPTagQueue
              {
                Tag = MultiProcessor
                Tag += 8Processors
                ...
              }
              # This queue has Tag = MultiProcessor and RequiredTag = MultiProcessor. So it will accept:
              # - jobs that require Tag = MultiProcessors (and no others)
              RequiredMPTagQueue
              {
                Tag = MultiProcessor
                RequiredTag = MultiProcessor
                ...
              }
              # This queue has Tag = [MultiProcessor, 8Processors] and RequiredTag = MultiProcessor. So it will accept:
              # - jobs that require both the tags above (and no others)
              # - jobs that require Tag = MultiProcessor (and no others)
              MultipleMPTagQueue
              {
                Tag = MultiProcessor
                Tag += 8Processors
                RequiredTag = MultiProcessor
                ...
              }
            }
          }
          # Tags can also be given to CEs. So, the following CE accepts ALSO MultiProcessor jobs.
          # The same examples above, which were done for the queues, apply also to CEs
          MP-CE.cern.ch
          {
            Tag = MultiProcessor
            Queues
            {
              some_queue
              {
                 ...
              }
            }
          }
        }
      }
