.. _tuto_install_wms:


========================================
Installing the WorkloadManagement System
========================================

.. set highlighting to console input/output
.. highlight:: console

Pre-Requisite
=============

You should:

* have a machine setup as described in :ref:`tuto_basic_setup`
* have installed two DIRAC SE using the tutorial (:ref:`tuto_install_dirac_se`).
* have followed the tutorial on identity management (:ref:`tuto_managing_identities`)
* have installed the TS using the tutorial (:ref:`tuto_install_ts`)

Tutorial Goal
=============

The aim of the tutorial is to install the WorkloadManagement system components and to use them to generate and submit a simple job.


More Links
==========

* :ref:`WMS`
* Information about the types and options of the :ref:`Computing Elements<CE>`
* Information about the user jobs using the DIRAC API: :ref:`user jobs<user_jobs_api>`

Installing the WorkloadManagementSystem
=======================================

.. highlight:: console

This section is to be executed as ``diracuser`` with the ``dirac_admin`` proxy (reminder: ``dirac-proxy-init -g dirac_admin``).

Basically, the WorkloadManagement System (WMS) needs the ``SiteDirector`` agent to install pilots on Computing Elements (CEs) as well as
different services and agents such as the ``JobManager``, the ``JobMonitoring`` and the ``Matcher`` to manage the jobs and their status. 
The executors are used to check the jobs and schedule them on Task Queues.

The WMS is no different than any other DIRAC system. The installation steps are thus very simple::

  [diracuser@dirac-tuto ~]$ dirac-proxy-init -g dirac_admin
  [diracuser@dirac-tuto ~]$ dirac-admin-sysadmin-cli --host dirac-tuto
  Pinging dirac-tuto...
  [dirac-tuto]> add instance WorkloadManagement Production
  Adding WorkloadManagement system as Production self.instance for MyDIRAC-Production self.setup to dirac.cfg and CS WorkloadManagement system instance Production added successfully
  [dirac-tuto]> restart *
  All systems are restarted, connection to SystemAdministrator is lost
  [dirac-tuto]> install db JobDB
  MySQL root password:
  Adding to CS WorkloadManagement/JobDB
  Database JobDB from DIRAC/WorkloadManagementSystem installed successfully
  [dirac-tuto]> install db JobLoggingDB
  MySQL root password:
  Adding to CS WorkloadManagement/JobLoggingDB
  Database JobLoggingDB from DIRAC/WorkloadManagementSystem installed successfully
  [dirac-tuto]> install db PilotAgentsDB
  MySQL root password:
  Adding to CS WorkloadManagement/PilotAgentsDB
  Database PilotAgentsDB from DIRAC/WorkloadManagementSystem installed successfully
  [dirac-tuto]> install db SandboxMetadataDB
  MySQL root password:
  Adding to CS WorkloadManagement/SandboxMetadataDB
  Database SandboxMetadataDB from DIRAC/WorkloadManagementSystem installed successfully
  [dirac-tuto]> install db TaskQueueDB
  MySQL root password:
  Adding to CS WorkloadManagement/TaskQueueDB
  Database TaskQueueDB from DIRAC/WorkloadManagementSystem installed successfully
  [dirac-tuto]> install service WorkloadManagement PilotManager
  Loading configuration template /home/diracuser/DiracInstallation/DIRAC/WorkloadManagementSystem/ConfigTemplate.cfg
  Adding to CS service WorkloadManagement/PilotManager
  service WorkloadManagement_PilotManager is installed, runit status: Run
  [dirac-tuto]> install service WorkloadManagement JobManager
  Loading configuration template /home/diracuser/DiracInstallation/DIRAC/WorkloadManagementSystem/ConfigTemplate.cfg
  Adding to CS service WorkloadManagement/JobManager
  service WorkloadManagement_JobManager is installed, runit status: Run
  [dirac-tuto]> install service WorkloadManagement JobMonitoring
  Loading configuration template /home/diracuser/DiracInstallation/DIRAC/WorkloadManagementSystem/ConfigTemplate.cfg
  Adding to CS service WorkloadManagement/JobMonitoring
  service WorkloadManagement_JobMonitoring is installed, runit status: Run
  [dirac-tuto]> install service WorkloadManagement JobStateUpdate
  Loading configuration template /home/diracuser/DiracInstallation/DIRAC/WorkloadManagementSystem/ConfigTemplate.cfg
  Adding to CS service WorkloadManagement/JobStateUpdate
  service WorkloadManagement_JobStateUpdate is installed, runit status: Run
  [dirac-tuto]> install service WorkloadManagement Matcher
  Loading configuration template /home/diracuser/DiracInstallation/DIRAC/WorkloadManagementSystem/ConfigTemplate.cfg
  Adding to CS service WorkloadManagement/Matcher
  service WorkloadManagement_Matcher is installed, runit status: Run
  [dirac-tuto]> install service WorkloadManagement OptimizationMind
  Loading configuration template /home/diracuser/DiracInstallation/DIRAC/WorkloadManagementSystem/ConfigTemplate.cfg
  Adding to CS service WorkloadManagement/OptimizationMind
  service WorkloadManagement_OptimizationMind is installed, runit status: Run
  [dirac-tuto]> install service WorkloadManagement SandboxStore
  Loading configuration template /home/diracuser/DiracInstallation/DIRAC/WorkloadManagementSystem/ConfigTemplate.cfg
  Adding to CS service WorkloadManagement/SandboxStore
  service WorkloadManagement_SandboxStore is installed, runit status: Run
  [dirac-tuto]> install service WorkloadManagement WMSAdministrator
  Loading configuration template /home/diracuser/DiracInstallation/DIRAC/WorkloadManagementSystem/ConfigTemplate.cfg
  Adding to CS service WorkloadManagement/WMSAdministrator
  service WorkloadManagement_WMSAdministrator is installed, runit status: Run
  [dirac-tuto]> install service Framework BundleDelivery
  Loading configuration template /home/diracuser/DiracInstallation/DIRAC/Framework/ConfigTemplate.cfg
  Adding to CS service WorkloadManagement/BundleDelivery
  service WorkloadManagement_BundleDelivery is installed, runit status: Run
  [dirac-tuto]> install service Framework Monitoring
  Loading configuration template /home/diracuser/DiracInstallation/DIRAC/Framework/ConfigTemplate.cfg
  Adding to CS service WorkloadManagement/Monitoring
  service WorkloadManagement_BundleDelivery is installed, runit status: Run
  [dirac-tuto]> install agent WorkloadManagement SiteDirector
  Loading configuration template /home/diracuser/DiracInstallation/DIRAC/WorkloadManagementSystem/ConfigTemplate.cfg
  Adding to CS service WorkloadManagement/SiteDirector
  agent WorkloadManagement_SiteDirector is installed, runit status: Run
  [dirac-tuto]> install agent WorkloadManagement JobCleaningAgent
  Loading configuration template /home/diracuser/DiracInstallation/DIRAC/WorkloadManagementSystem/ConfigTemplate.cfg
  Adding to CS service WorkloadManagement/JobCleaningAgent
  agent WorkloadManagement_JobCleaningAgent is installed, runit status: Run
  [dirac-tuto]> install agent WorkloadManagement PilotStatusAgent
  Loading configuration template /home/diracuser/DiracInstallation/DIRAC/WorkloadManagementSystem/ConfigTemplate.cfg
  Adding to CS service WorkloadManagement/PilotStatusAgent
  agent WorkloadManagement_PilotStatusAgent is installed, runit status: Run
  [dirac-tuto]> install agent WorkloadManagement StalledJobAgent
  Loading configuration template /home/diracuser/DiracInstallation/DIRAC/WorkloadManagementSystem/ConfigTemplate.cfg
  Adding to CS service WorkloadManagement/StalledJobAgent
  agent WorkloadManagement_StalledJobAgent is installed, runit status: Run
  [dirac-tuto]> install executor WorkloadManagement Optimizers
  Loading configuration template /home/diracuser/DiracInstallation/DIRAC/WorkloadManagementSystem/ConfigTemplate.cfg
  Adding to CS service WorkloadManagement/Optimizers
  Loading configuration template /home/diracuser/DiracInstallation/DIRAC/WorkloadManagementSystem/ConfigTemplate.cfg
  Adding to CS service WorkloadManagement/JobPath
  Loading configuration template /home/diracuser/DiracInstallation/DIRAC/WorkloadManagementSystem/ConfigTemplate.cfg
  Adding to CS service WorkloadManagement/JobSanity
  Loading configuration template /home/diracuser/DiracInstallation/DIRAC/WorkloadManagementSystem/ConfigTemplate.cfg
  Adding to CS service WorkloadManagement/InputData
  Loading configuration template /home/diracuser/DiracInstallation/DIRAC/WorkloadManagementSystem/ConfigTemplate.cfg
  Adding to CS service WorkloadManagement/JobScheduling
  executor WorkloadManagement_Optimizers is installed, runit status: Run
  [dirac-tuto]> restart WorkloadManagement *

Create and submit a job
=======================

This section is to be executed as ``diracuser`` with the ``dirac_user`` proxy (reminder: ``dirac-proxy-init``).

Create a Python script to generate and submit a simple job. Copy paste the following lines into a new file called ``job.py``

.. code-block:: python

  #!/bin/env python
  # Magic lines necessary to activate the DIRAC Configuration System
  # to discover all the required services
  from DIRAC.Core.Base import Script
  Script.parseCommandLine(ignoreErrors=True)
  from DIRAC.Interfaces.API.Job import Job
  from DIRAC.Interfaces.API.Dirac import Dirac
  
  j = Job()
  dirac = Dirac()

  j.setName('MyFirstJob')
  j.setJobGroup('MyJobs')
  
  # Specify CPU requirements
  j.setCPUTime(21600)
  
  # Specify the log level of the job execution: INFO (default), DEBUG, VERBOSE
  j.setLogLevel('DEBUG')
  
  # Executabe and arguments can be given in one call
  j.setExecutable('echo', arguments='Hello world!')
  
  result = dirac.submitJob(j)
  if not result['OK']:
      print("ERROR:", result['Message'])
  else:
      print(result['Value'])

This script creates a new job called ``MyFirstJob`` and aims at executing ``echo "Hello World!"``. The output should be something like that::

  [diracuser@dirac-tuto ~]$ python job.py
  <jobid>
  [diracuser@dirac-tuto ~]$ dirac-wms-job-status <jobid>
  JobID=<jobid> Status=Waiting; MinorStatus=Pilot Agent Submission; Site=ANY; 

As we have not defined any CE yet, the job cannot run and remains ``Waiting``.

Adding a CE
===========

First, as ``root``, we create a new user ``diracpilot`` that is going to simulate an SSH Computing Element on ``dirac-tuto``::

    adduser -s /bin/bash -d /home/diracpilot diracpilot
    echo password | /usr/bin/passwd --stdin diracpilot

As ``diracuser``, connect to ``diracpilot`` through SSH a first time to initialize the connection and make sure everything works::

    ssh diracpilot@dirac-tuto

Then, as ``diracuser`` with the ``dirac_admin`` proxy, we need to define a CE in a ``/Resources/Sites/<Grid>/<Site>`` section of the configuration file using the WebApp (create the sections if necessary)::

  Resources
  {
    Sites
    {
      MyGrid
      {
        MyGrid.Site1.uk
        {
          CE = dirac-tuto
          CEs
          {
            dirac-tuto
            {
              CEType = SSH
              SSHHost = dirac-tuto
              SSHUser = diracpilot
              SSHPassword = password
              SSHType = ssh
              Queues
              {
                queue
                {
                  CPUTime = 40000
                  MaxTotalJobs = 5
                  MaxWaitingJobs = 10
                  BundleProxy = True
                  BatchError = /home/diracpilot/localsite/error
                  ExecutableArea = /home/diracpilot/localsite/submission
                  RemoveOutput = True
                }
              }
            }
          }
        }
      }
    }
  }

We set the type of the CE, ``SSH`` in our case, as well as the required parameters to access the Element. 
Then we configure the queue that is going to receive the jobs. A queue corresponds to a set of Worker Nodes in practice.

Note: make sure the ``CPUTime`` of the queue is above the ``CPUTime`` of the job, else the job will not be scheduled to run on this Worker Node.

Configuring the pilots
======================

A job is not able to run directly on a Worker Node and needs to be executed by a pilot that has the knowledge of its environment and knows how to run jobs within it.
The pilot is the first job to be deployed on a Worker Node and it installs and configures DIRAC and asks for pending jobs in Task Queues that would match the environment of the Worker Node. Add the following lines in the ``/Operations/MyDIRAC-Production`` section using the WebApp::

  Pilot
  {
    Version = v7r0p36
    CheckVersion = False
    Command
    {
      Test = GetPilotVersion
      Test += CheckWorkerNode
      Test += InstallDIRAC
      Test += ConfigureBasics
      Test += ConfigureCPURequirements
      Test += ConfigureArchitecture
      Test += CheckCECapabilities
      Test += LaunchAgent
    }
    GenericPilotGroup = dirac_user
    GenericPilotUser = ciuser
    pilotFileServer = dirac-tuto:8443
  }

We pass our credentials information to the pilot so that it can interact with DIRAC as it needs to execute the commands defined in ``Commands``.
Only a small script called ``pilotWrapper`` is directly passed to the CE, most of the files used by the pilot will be downloaded from ``pilotFileServer`` during the script execution.
These files can be uploaded and updated at each commit done to the configuration, we just need to create the directory that is going to contain the files required by the pilot and add the information within the configuration. First, add the option below to the configuration, in the ``/WebApp`` section::

    StaticDirs = pilot

Due to installation issue, the synchronization CS-Pilot is disabled by default, modify the ``/Operations/Defaults/Pilot`` section in the ``/opt/dirac/etc/dirac.cfg`` file as ``dirac``::

    UpdatePilotCStoJSONFile = True

Still as ``dirac``, create the pilot repository that will contain all the pilot files that will be updated whenever a CS update is triggered::

    mkdir -p /opt/dirac/webRoot/www/pilot


 ..warning:: Do not put the Pilot configuration in ``Operations/Defaults``, DIRAC would not be able to get it.

Configuring the Sandbox
=======================

We need to define a Sandbox to pass input files related to the job to the Worker Node and then to get the results of the execution.
A Sandbox is represented as a StorageElement and can be installed in this way. As ``diracuser`` with the ``dirac_admin`` proxy, executes ::

    [diracuser@dirac-tuto ~]$ dirac-admin-sysadmin-cli --host dirac-tuto
    Pinging dirac-tuto...
    [dirac-tuto]> install service DataManagement ProductionSandboxSE -m StorageElement -p Port=9146 -p BasePath=/opt/dirac/storage/sandboxes

Then the following lines have to be added to the configuration in the ``/Resources/StorageElements`` section using the WebApp::

  ProductionSandboxSE
  {
    BackendType = DISET
    DIP
    {
      Host = dirac-tuto
      Port = 9146
      Protocol = dips
      Path = /DataManagement/ProductionSandboxSE
      Access = remote
    }
  }

The Storage Element is then used by the ``SandboxStore`` service. 
If it is not defined (it should in practice), add the following option in ``Systems/WorkloadManagement/Production/Services/SandboxStore``::

    LocalSE = ProductionSandboxSE

Make the Site available for receiving jobs
==========================================

By default, the Site previously created is not allowed to receive any job from DIRAC. Execute the following command to add it to the list of available Sites::

  [diracuser@dirac-tuto ~]$ dirac-admin-allow-site MyGrid.Site1.uk "test" -E False
  Site MyGrid.Site1.uk status is set to Active

Finally restart the WorkloadManagement system to apply the configuration changes to the components::

  [diracuser@dirac-tuto ~]$ dirac-admin-sysadmin-cli --host dirac-tuto
  Pinging dirac-tuto...
  [dirac-tuto]> restart WorkloadManagement *

After a moment we should get a result performing these commands::
    
  [diracuser@dirac-tuto ~]$ dirac-wms-job-status <job_id>
  JobID=<jobid> Status=Done; MinorStatus=Execution Complete; Site=MyGrid.Site1.uk;
  [diracuser@dirac-tuto ~]$ dirac-wms-job-get-output <job_id>
  Job output sandbox retrieved in /home/diracuser/<job_id>/
