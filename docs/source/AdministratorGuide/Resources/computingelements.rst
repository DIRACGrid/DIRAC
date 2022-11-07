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
of the *ComputingElement* is located in the inside the corresponding site section in the
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


HTCondor Computing Element
@@@@@@@@@@@@@@@@@@@@@@@@@@@@@

A commented example follows::

   # Section placed in the */Resources/Sites/<domain>/<site>/CEs* directory
   ce01.infn.it
   {
     CEType = HTCondorCE

     Queues
     {
       # The queue section name should be the same as in the BDII description
       long
       {
         # Max CPU time in HEP'06 unit secs
         maxCPUTime = 10000
         # Max total number of jobs in the queue
         MaxTotalJobs = 5
         # Max number of waiting jobs in the queue
         MaxWaitingJobs = 2
       }
     }
   }



Cloud Computing Element
@@@@@@@@@@@@@@@@@@@@@@@
The CloudComputingElement allows submission to cloud sites using libcloud
(via the standard SiteDirector agent). The instances are contextualised using
cloud-init. Please see :mod:`~DIRAC.Resources.Computing.CloudComputingElement`
for setup and configuration.



SSH Computing Element
@@@@@@@@@@@@@@@@@@@@@

The SSHComputingElement is used to submit pilots through an SSH tunnel to
computing clusters with various batch systems. A commented example of its
configuration follows ::

   # Section placed in the */Resources/Sites/<domain>/<site>/CEs* directory
   pc.farm.ch
   {
     CEType = SSH
     # Type of the local batch system. Available batch system implementations are:
     # Torque, Condor, GE, LSF, OAR, SLURM
     BatchSystem = Torque
     SSHHost = pc.domain.ch
     # SSH connection details to be defined in the local configuration
     # of the corresponding SiteDirector
     SSHUser = dirac_ssh
     SSHPassword = XXXXXXX
     # Alternatively, the private key location can be specified instead
     # of the SSHPassword
     SSHKey = /path/to/the/key
     # SSH port if not standard one
     SSHPort = 222
     # Sometimes we need an extra tunnel where the batch system is on accessible
     # directly from the site gateway host
     SSHTunnel = ssh pcbatch.domain.ch
     # SSH type: ssh (default) or gsissh
     SSHType = ssh
     # Options to SSH command
     SSHOptions = -o option1=something -o option2=somethingelse
     # Queues section contining queue definitions
     Queues
     {
       # The queue section name should be the same as the name of the actual batch queue
       long
       {
         # Max CPU time in HEP'06 unit secs
         maxCPUTime = 10000
         # Max total number of jobs in the queue
         MaxTotalJobs = 5
         # Max number of waitin jobs in the queue
         MaxWaitingJobs = 2
         # Flag to include pilot proxy in the payload sent to the batch system
         BundleProxy = True
         # Directory on the CE site where the pilot standard output stream will be stored
         BatchOutput = /home/dirac_ssh/localsite/output
         # Directory on the CE site where the pilot standard output stream will be stored
         BatchError = /home/dirac_ssh/localsite/error
         # Directory where the payload executable will be stored temporarily before
         # submission to the batch system
         ExecutableArea = /home/dirac_ssh/localsite/submission
         # Extra options to be passed to the qsub job submission command
         SubmitOptions =
         # Flag to remove the pilot output after it was retrieved
         RemoveOutput = True
       }
     }
   }



SSHBatch Computing Element
@@@@@@@@@@@@@@@@@@@@@@@@@@

This is an extension of the SSHComputingElement capable of submitting several jobs on one host.

Like all SSH Computing Elements, it's defined like the following::

   # Section placed in the */Resources/Sites/<domain>/<site>/CEs* directory
   pc.farm.ch
   {
     CEType = SSHBatch

     # Parameters of the SSH conection to the site. The /2 indicates how many cores can be used on that host.
     # It's equivalent to the number of jobs that can run in parallel.
     SSHHost = pc.domain.ch/2
     SSHUser = dirac_ssh
     # if SSH password is not given, the public key connection is assumed.
     # Do not put this in the CS, put it in the local dirac.cfg of the host.
     # You don't want external people to see the password.
     SSHPassword = XXXXXXXXX
     # If no password, specify the key path
     SSHKey = /path/to/key.pub
     # In case your SSH connection requires specific attributes (see below) available in late v6r10 versions (TBD).
     SSHOptions = -o option1=something -o option2=somethingelse

     Queues
     {
       # Similar to the corresponding SSHComputingElement section
     }
   }


The ``SSHOptions`` is needed when for example the user used to run the agent isn't local and requires access to afs. As the way the agents are started isn't a login, they does not
have access to afs (as they have no token), so no access to the HOME directory. Even if the HOME environment variable is replaced, ssh still looks up the original home directory.
If the ssh key and/or the known_hosts file is hosted on afs, the ssh connection is likely to fail. The solution is to pass explicitely the options to ssh with the SSHOptions option.
For example::

    SSHOptions = -o UserKnownHostsFile=/local/path/to/known_hosts

allows to have a local copy of the ``known_hosts`` file, independent of the HOME directory.



InProcessComputingElement
@@@@@@@@@@@@@@@@@@@@@@@@@

The InProcessComputingElement is usually invoked by a JobAgent to execute user
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

PoolComputingElement
@@@@@@@@@@@@@@@@@@@@

The Pool Computing Element is used on multi-processor nodes, e.g. cloud VMs
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
