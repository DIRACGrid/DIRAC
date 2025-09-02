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
        # Site section. This is the DIRAC's site name.
        LCG.CNAF.it
        {
          # Alternative site name (e.g. site name in GOC DB)
          Name = CNAF

          # Section describing each CE
          CEs
          {
            # Specific CE description section. This site name is unique.
            ce01.infn.it
            {
              # Type of the CE. "HTCondorCE" and "AREX" and "SSH" are the most common types.
              CEType = HTCondorCE

              # Section to describe various (logical) queues in the CE.
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

Selecting the container image
@@@@@@@@@@@@@@@@@@@@@@@@@@@@@

The :mod:`~DIRAC.Resources.Computing.SingularityComputingElement` CE and the
:py:mod:`~DIRAC.Core.scripts.dirac_apptainer_exec` command resolve the container image with
:py:func:`~DIRAC.Core.Utilities.ContainerImageResolver.resolveImagePath`, which supports the CVMFS multiarch layout so
that the same configuration works on nodes of different architectures.

The following options are read from the ``Singularity`` section (or from the CE parameters, which take precedence).
The image references shown are examples: a site normally points them at the image its VO builds and publishes.

- ImageReference (str) - the OCI reference of the image, e.g. ``registry.hub.docker.com/library/ubuntu:24.04``. It must
                         be a *relative* reference: absolute paths and ``..`` are rejected, since they would bypass both
                         the base path and the architecture directory.
- ImageBasePath (str) - the root of the multiarch image repository. Defaults to ``/cvmfs/unpacked.cern.ch/.multiarch``.
- ContainerRoot (str) - **deprecated**, kept for backward compatibility. A path to a single architecture (in practice
                        x86_64) image, used as a fallback when no multiarch image is found. Defaults to
                        ``/cvmfs/cernvm-prod.cern.ch/cvm4``.

The image is looked up at ``<ImageBasePath>/<architecture>/<ImageReference>``, where the architecture is the OCI
(GOARCH) name of the node, with the variant appended after a colon where there is one: ``amd64``, ``arm64``,
``arm:v7``, ``386``, ``ppc64le``, etc. The repository also publishes symlinks for the usual ``uname -m`` names
(``x86_64``, ``aarch64``, ``i386`` ...), so an architecture DIRAC does not know about is used as reported.

Some architectures are published under more than one directory name, because the name comes from the image manifest:
an image declaring ``arm64`` with no variant is published under ``arm64``, one declaring variant ``v8`` under
``arm64:v8``, and the publisher only symlinks one to the other when the plain name is not already a real directory.
Those names are therefore tried in turn -- ``arm64`` then ``arm64:v8`` on an ARM64 node, ``arm:v7`` then ``arm`` on an
ARMv7 one -- and the first one that exists is used. When none of them does, the warning names every path that was
tried, together with the directories the repository actually publishes for that architecture, so that a variant DIRAC
does not yet know about is visible in the log rather than silently degrading to ``ContainerRoot``. Such a directory is
only reported, never used: variant compatibility is directional (an ``arm:v6`` image runs on an ARMv7 node, an
``arm:v7`` image does not run on an ARMv6 one), so an unrecognised variant cannot be assumed to run.

If no multiarch path exists, the deprecated ``ContainerRoot`` is used instead and a warning is logged, naming the
architecture and every path that was tried.

``ContainerRoot`` images are built for a single architecture, in practice ``amd64``. On a node whose architecture DIRAC
recognises as a different one, the image is therefore *not* used: resolution fails with an error and the payload is not
submitted, rather than started only to die with an exec format error. Publish the image under ``ImageBasePath`` for that
architecture to fix it. When the node reports an architecture DIRAC does not recognise, it may still be a compatible
one, so ``ContainerRoot`` is used and a warning logged instead.

::

  Resources
  {
    Computing
    {
      Singularity
      {
        ImageReference = registry.hub.docker.com/library/ubuntu:24.04
        # ImageBasePath = /cvmfs/unpacked.cern.ch/.multiarch
      }
    }
  }

``dirac-apptainer-exec`` accepts a ``-i``/``--image`` option overriding the configuration. That value is used on its
own: an existing local path is taken as-is, and otherwise it is looked up as an OCI reference in the multiarch layout.
``ContainerRoot`` is not consulted in that case, so the command never runs an image other than the one asked for.
Unlike the CE, it has no built-in default image: if nothing is configured and nothing is found, the command fails.

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

Debugging Computing Element Issues
@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@

When troubleshooting Computing Element connectivity or job submission problems, you can use the
:py:mod:`~DIRAC.WorkloadManagementSystem.scripts.dirac_admin_debug_ce` command to systematically test CE interactions.

This command validates CE functionality by testing status retrieval, job submission, monitoring, and output collection.
For detailed usage instructions, prerequisites, and examples, run ``dirac-admin-debug-ce --help``.
