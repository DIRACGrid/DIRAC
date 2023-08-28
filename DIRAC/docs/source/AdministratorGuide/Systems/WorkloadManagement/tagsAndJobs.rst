.. _tagsAndJobs:

============================================
The generic Tags mechanism for jobs matching
============================================


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
              # This queue has Tag = GPU. So it will accept:
              # - jobs that require Tag = GPU (and no others)
              # - jobs that require no Tags
              GPUTagQueue
              {
                Tag = GPU
                ...
              }
              # This queue has Tag = [GPU, NVidiaGPU]. So it will accept:
              # - jobs that require both the tags above (and no others)
              # - jobs that require Tag = GPU (and no others)
              # - jobs that require Tag = NVidiaGPU (and no others)
              # - jobs that require no Tags
              MultipleGPUTagQueue
              {
                Tag = GPU
                Tag += NVidiaGPU
                ...
              }
              # This queue has Tag = GPU and RequiredTag = GPU. So it will accept:
              # - jobs that require Tag = GPUs (and no others)
              RequiredGPUTagQueue
              {
                Tag = GPU
                RequiredTag = GPU
                ...
              }
              # This queue has Tag = [GPU, NVidiaGPU] and RequiredTag = GPU. So it will accept:
              # - jobs that require both the tags above (and no others)
              # - jobs that require Tag = GPU (and no others)
              MultipleGPUTagQueue
              {
                Tag = GPU
                Tag += NVidiaGPU
                RequiredTag = GPU
                ...
              }
            }
          }
          # Tags can also be given to CEs. So, the following CE accepts ALSO GPU jobs.
          # The same examples above, which were done for the queues, apply also to CEs
          GPU-CE.cern.ch
          {
            Tag = GPU
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
