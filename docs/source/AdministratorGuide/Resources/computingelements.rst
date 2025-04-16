.. _CE:

==================
Computing Elements
==================

Direct access to the site computing clusters is done by sending pilot jobs in a similar way as
it is done for the grid sites. The pilot jobs are sent by a specialized agent called *SiteDirector*.

The :py:mod:`~DIRAC.WorkloadManagementSystem.Agent.SiteDirector` is part of the agents of the Workload Management System, and can't work alone.
Please refer to :ref:`documentation of the WMS <WMSArchitecture>` for info about the other WMS components.

The *SiteDirector* is usually serving one or several sites and can run as part of the central service
installation or as an on-site component. At the initialization phase it gets description of the site's
capacity and then runs in a loop performing the following operations:

- Check if there are tasks in the DIRAC TaskQueue eligible for running on the site;
- If there are tasks to run, check the site current occupancy in terms of numbers of already running
  or waiting pilot jobs;
- If there is a spare capacity on the site, submit a number of pilot jobs corresponding to the
  number of user jobs in the TaskQueue and the number of slots in the site computing cluster;
- Monitor the status of submitted pilot jobs, update the PilotAgentsDB accordingly;
- Retrieve the standard output/error of the pilot jobs.

*SiteDirector* is submitting pilot jobs with credentials of a user entitled to run *generic* pilots
for the given user community. The *generic* pilots are called so as they are capable of executing
jobs on behalf of community users.

SiteDirector Configuration
--------------------------

The *SiteDirector* configuration is defined in the standard way as for any DIRAC agent. It belongs
to the WorkloadManagement System and its configuration section is:

   /Systems/WorkloadManagement/<instance>/Agents/SiteDirector

For detailed information on the CS configuration of the SiteDirector,
please refer to the WMS :ref:`Code Documentation<code_documentation>`.



Computing Elements
-------------------

DIRAC can use different computing resources via specialized clients called *ComputingElements*.
Each computing resource is accessed using an appropriate :mod:`~DIRAC.Resources.Computing` class derived from a common
base class.

The *ComputingElements* should be properly described to be useful. The configuration
of the *ComputingElement* is located inside the corresponding site section in the
/Resources section. An example of a site description is given below::

  Resources
  {
    Sites
    {
      # Site administrative domain
      LCG
      {
        # Site section
        LCG.CNAF.it
        {
          # Site name
          Name = CNAF

          # List of valid CEs on the site
          CE = ce01.infn.it, ce02.infn.it

          # Section describing each CE
          CEs
          {
            # Specific CE description section
            ce01.infn.it
            {
              # Type of the CE
              CEType = HTCondorCE

              # Section to describe various queue in the CE
              Queues
              {
                long
                {
                  ...
                }
              }
            }
          }
        }
      }
    }
  }


This is the general structure in which specific CE descriptions are inserted.
The CE configuration is part of the general DIRAC configuration
It can be placed in the general Configuration Service or in the local configuration of the DIRAC installation.
Examples of the configuration can be found in the :ref:`full_configuration_example`, in the *Resources/Computing* section.
You can find the options of a specific CE in the code documentation: :mod:`DIRAC.Resources.Computing`.

Some CE parameters are confidential, e.g.
password of the account used for the SSH tunnel access to a site. The confidential parameters
should be stored in the local configuration in protected files.

The *SiteDirector* is getting the CE descriptions from the configuration and uses them according
to their specified capabilities and preferences. Configuration options specific for different types
of CEs are describe in the subsections below

Note that there's no absolute need to define a 1-to-1 relation between CEs and Queues in DIRAC and "in real".
If for example you want to send, to the same queue, a mix of single processor and multiprocessor Pilots,
you can define two queues identical but for the NumberOfProcessors parameter. To avoid sending single
processor jobs to multiprocessor queues, add the ``RequiredTag=MultiProcessor`` option to a multiprocessor queue. To
automatically create the equivalent single core queues, see the :mod:`~DIRAC.ConfigurationSystem.Agent.Bdii2CSAgent`
configuration.

Interacting with Grid Sites
@@@@@@@@@@@@@@@@@@@@@@@@@@@
The :mod:`~DIRAC.Resources.Computing.HTCondorCEComputingElement` and the :mod:`~DIRAC.Resources.Computing.AREXComputingElement` eases
the interactions with grid sites, by managing pilots using the underlying batch systems.
Instances of such CEs are generally setup by the site administrators.


Leveraging Opportunistic computing clusters
@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
Sites that do not manage CEs can generally still be accessed via SSH.
The :mod:`~DIRAC.Resources.Computing.SSHComputingElement` and :mod:`~DIRAC.Resources.Computing.SSHBatchComputingElement`
can be used to submit pilots through an SSH tunnel to computing clusters with various batch systems: :mod:`~DIRAC.Resources.Computing.BatchSystems`.


Dealing with the Cloud resources
@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
The :mod:`~DIRAC.Resources.Computing.CloudComputingElement` allows submission to cloud sites using libcloud
(via the standard SiteDirector agent). The instances are contextualised using cloud-init.


Computing Elements within allocated computing resources
@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
The :mod:`~DIRAC.Resources.Computing.InProcessComputingElement` is usually invoked by a Pilot-Job (JobAgent agent) to execute user
jobs in the same process as the one of the JobAgent. Its configuration options
are usually defined in the local configuration /Resources/Computing/CEDefaults
section ::

  Resources
  {
    Computing
    {
      CEDefaults
      {
        NumberOfProcessors = 2
        Tag = MultiProcessor
        RequiredTag = MultiProcessor
      }
    }
  }


The :mod:`~DIRAC.Resources.Computing.PoolComputingElement` is used on multi-processor nodes, e.g. cloud VMs
and can execute several user payloads in parallel using an internal ProcessPool.
Its configuration is also defined by pilots locally in the /Resources/Computing/CEDefaults
section ::

  Resources
  {
    Computing
    {
      CEDefaults
      {
        NumberOfProcessors = 2
        Tag = MultiProcessor
        RequiredTag = MultiProcessor
        # The MultiProcessorStrategy flag defines if the Pool Computing Element
        # will generate several descriptions to present possibly several queries
        # to the Matcher in each cycle trying to select multi-processor jobs first
        # and, if no match found, simple jobs finally
        MultiProcessorStrategy = True
      }
    }
  }

Applying cgroup2 limits to computing resources
@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@

Both the :mod:`~DIRAC.Resources.Computing.InProcessComputingElement` and
:mod:`~DIRAC.Resources.Computing.SingularityComputingElement` CEs support applying Linux cgroup2 CPU and memory limits to
the slot. These will be applied if the site allows cgroup2 delegation, if this is not available execution will continue
without the limits. The limit values can be specified using the following CE parameters (all settings are optional and can
be left undefined if not needed):

- CPULimit (float) - The number of cores that the job may use. Usage beyond this will be throttled.
- MemoryLimitMB (int) - The memory limit for the job in MB. Usage beyond this will trigger the out-of-memory killer
                        considering processes within the slot.
- MemoryNoSwap (bool) - If yes or true, the job will not be allowed to use swap memory. Swap memory is not included
                        in the main memory limit.

Note that the memory limit should be lower than the amount requested with the submission CE in order to allow the main
pilot processes to be protected. For example if you request 4096M (e.g. via XRSL) at submission, around 150M is needed
for the pilot, so a limit of 3950M would be recommended.

These can be specified in the CEDefaults section to apply a standardised slot size limit::

  Resources
  {
    Computing
    {
      CEDefaults
      {
        CPULimit = 1.0
        MemoryLimitMB = 3950
        MemoryNoSwap = True
      }
    }
  }
