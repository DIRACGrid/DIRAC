=====================
Transformation System
=====================

.. toctree::
   :maxdepth: 2

.. contents:: Table of contents
   :depth: 4


The Transformation System (TS) is used to automatise common tasks related to production activities.
Just to make some baic examples, the TS can handle the generation of Simulation jobs,
or Data Re-processing jobs as soon as a 'pre-defined' data-set is available,
or Data Replication to 'pre-defined' SE destinations as soon as the first replica is registered in the Catalog.

The lingo used here needs a little explanation:
throughout this document the terms "transformation" and "production" are often used to mean the same thing:

- A *"production"* is a transformation managed by the TS that is a "Data Processing" transformation (e.g. Simulation, Merge, DataReconstruction...). A Production ends up creating jobs in the WMS.
- A "Data Manipulation" transformation replicates, or remove, data from storage elements. A "Data Manipulation" transformation ends up creating requests in the RMS (Request Management System).

For each high-level production task, the production manager creates a transformation.
Each transformation can have different parameters. The main parameters of a Transformation are the following:

- Type (*e.g.* Simulation, DataProcessing, Removal, Replication)
- Plugin (Standard, BySize, etc.)
- The possibility of having Input Files.

Within the TS a user can (for example):

- Generate several identical tasks, differing by few parameters (e.g. Input Files list)
- Extend the number of tasks
- have one single high-level object (the Transformation) associated to a given production for global monitoring

Disadvantages:

- For very large installations, the submission may be percieved as slow, since there is no use (not yet) of Parametric jobs.

Several improvements have been made in the TS to handle scalability, and extensibility issues.
While the system structure remains intact, "tricks" like threading and caching have been extensively applied.

It's not possible to use ISB (Input Sandbox) to ship local files as for 'normal' Jobs (this should not be considered, anyway, a disadvantage).

-------------
Configuration
-------------

* **Operations**

  * In the Operations/[VO]/Transformations section, *Transformation Types* must be added
  * By default, the WorkflowTaskAgent will treat all the *DataProcessing* transformations and the RequestTaskAgent all the *DataManipulation* ones
  * An example of working configuration is give below::

        Transformations
        {
          DataProcessing = MCSimulation
          DataProcessing += CorsikaRepro
          DataProcessing += Merge
          DataProcessing += Analysis
          DataProcessing += DataReprocessing
          DataManipulation = Removal
          DataManipulation += Replication
        }

* **Agents**

  * Agents must be configured in the Systems/Transformation/[VO]/Agents section
  * The *Transformation Types* to be treated by the agent must be configured if and only if they are different from those set in the 'Operations' section. This is useful, for example, in case one wants several agents treating different transformation types, *e.g.*: one WorkflowTaskAgent for DataReprocessing transformations, a second for Merge and MCStripping, etc. Advantage is speedup.
  * For the WorkflowTaskAgent and RequestTaskAgent some options must be added manually
  * An example of working configuration is give below, where 2 specific WorkflowTaskAgents, each treating a different subset of transformation types have been added. Also notice the shifterProxy set by each one.

  ::

        WorkflowTaskAgent
        {
          #Transformation types to be treated by the agent
          TransType = MCSimulation
          TransType += DataReconstruction
          TransType += DataStripping
          TransType += MCStripping
          TransType += Merge
          TransType += DataReprocessing
          #Task statuses considered transient that should be monitored for updates
          TaskUpdateStatus = Submitted
          TaskUpdateStatus += Received
          TaskUpdateStatus += Waiting
          TaskUpdateStatus += Running
          TaskUpdateStatus += Matched
          TaskUpdateStatus += Completed
          TaskUpdateStatus += Failed
          shifterProxy = ProductionManager
          #Flag to eanble task submission
          SubmitTasks = yes
          #Flag for checking reserved tasks that failed submission
          CheckReserved = yes
          #Flag to enable task monitoring
          MonitorTasks = yes
          PollingTime = 120
          MonitorFiles = yes
        }
        WorkflowTaskAgent-RealData
        {
          #@@-phicharp@lhcb_admin - 2015-06-05 16:44:11
          TransType = DataReconstruction
          TransType += DataStripping
          shifterProxy = DataProcessing
          LoadName = WorkflowTaskAgent-RealData
          Module = WorkflowTaskAgent
        }
        WorkflowTaskAgent-Simulation
        {
          #@@-phicharp@lhcb_admin - 2015-06-05 16:44:11
          TransType = Simulation
          TransType += MCSimulation
          shifterProxy = SimulationProcessing
          LoadName = WorkflowTaskAgent-RealData
          Module = WorkflowTaskAgent
        }
        RequestTaskAgent
        {
          PollingTime = 120
          SubmitTasks = yes
          CheckReserved = yes
          MonitorTasks = yes
          MonitorFiles = yes
          TaskUpdateStatus = Submitted
          TaskUpdateStatus += Received
          TaskUpdateStatus += Waiting
          TaskUpdateStatus += Running
          TaskUpdateStatus += Matched
          TaskUpdateStatus += Completed
          TaskUpdateStatus += Failed
          TransType = Removal
          TransType += Replication
        }

-------
Plugins
-------

There are two different types of plugins, i.e. TransformationAgent plugins and TaskManager plugins. The first are used to 'group' the input files of the tasks according to different criteria, while the latter are used to specify the tasks destinations.

TransformationAgent plugins
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* Standard: group files by replicas (tasks create based on the file location)
* BySize: group files until they reach a certain size (Input size in Gb)
* ByShare: group files given the share (specified in the CS) and location
* Broadcast: take files at the source SE and broadcast to a given number of locations (used for replication)

TaskManager plugins
^^^^^^^^^^^^^^^^^^^^

By default the standard plugin (BySE) sets job's destination depending on the location of its input data. Starting from v6r13 a new **ByJobType**
TaskManager plugin has been introduced, so that different rules for site destinations can be specified for each JobType.
In order to use the ByJobType plugin, one has to:

* Set CS section Operations/Transformations/DestinationPlugin = ByJobType
* Set the JobType in the job workflow of the transformation, *e.g.*:


  ::

        from DIRAC.TransformationSystem.Client.Transformation import Transformation
        from DIRAC.Interfaces.API.Job import Job

        t = Transformation()
        job = Job()
        ...

        job.setType('DataReprocessing')
        t.setBody ( job.workflow.toXML() )

* Define the actual rules for each JobType in the CS section Operation/JobTypeMapping, as in the following example:

  ::

        JobTypeMapping
        {
          AutoAddedSites = LCG.CERN.ch
          AutoAddedSites += LCG.IN2P3.fr
          AutoAddedSites += LCG.CNAF.it
          AutoAddedSites += LCG.PIC.es
          AutoAddedSites += LCG.GRIDKA.de
          AutoAddedSites += LCG.RAL.uk
          AutoAddedSites += LCG.SARA.nl
          AutoAddedSites += LCG.RRCKI.ru
          DataReprocessing
          {
            Exclude = ALL
            Allow
            {
              LCG.NIKHEF.nl = LCG.SARA.nl
              LCG.UKI-LT2-QMUL.uk = LCG.RAL.uk
              LCG.CPPM.fr = LCG.SARA.nl
              LCG.USC.es = LCG.PIC.es
              LCG.LAL.fr = LCG.CERN.ch
              LCG.LAL.fr += LCG.IN2P3.fr
              LCG.BariRECAS.it = LCG.CNAF.it
              LCG.CBPF.br = LCG.CERN.ch
              VAC.Manchester.uk = LCG.RAL.uk
            }
          }
          Merge
          {
            Exclude = ALL
            Allow
            {
              LCG.NIKHEF.nl = LCG.SARA.nl
            }
          }
        }


  * By default, all sites are allowed to do every job
  * "AutoAddedSites" contains the list of sites allowed to run jobs with files in their local SEs
  * Sections under "JobTypeMapping" correspond to the different JobTypes one may want to define, *e.g.*: DataReprocessing, Merge, etc.
  * For each JobType one has to define:

    * "Exclude": the list of sites that will be removed as destination sites ("ALL" for all sites)
    * "Allow": the list of 'helpers', specifying sites helping another site

  * In the example above all sites in "AutoAddedSites" are allowed to run jobs with input files in their local SEs.
  These sites won't be excluded, even if set in the Exclude list.
  For DataReprocessing jobs, jobs having input files at LCG.NIKHEF.nl local SEs can run both at LCG.NIKHEF.nl and at LCG.SARA.nl, etc.

---------
Use-cases
---------

MC Simulation
^^^^^^^^^^^^^^
Generation of many identical jobs which don't need Input Files and having as varying parameter a variable built from @{JOB_ID}.

* **Agents**

  ::

    WorkflowTaskAgent, MCExtensionAgent (optional)

The WorkflowTaskAgent uses the TaskManager client to transform a 'Task' into a 'Job'.

* Example:

  ::

    from DIRAC.TransformationSystem.Client.Transformation import Transformation
    from DIRAC.Interfaces.API.Job import Job
    j = myJob()
    ...
    t = Transformation( )
    t.setTransformationName("MCProd") # This must be unique
    t.setTransformationGroup("Group1")
    t.setType("MCSimulation")
    t.setDescription("MC prod example")
    t.setLongDescription( "This is the long description of my production" ) #mandatory
    t.setBody ( j.workflow.toXML() )
    t.addTransformation() #transformation is created here
    t.setStatus("Active")
    t.setAgentType("Automatic")

Re-processing
^^^^^^^^^^^^^^

Generation of identical jobs with Input Files.

* **Agents**

  ::

    TransformationAgent, WorkflowTaskAgent, InputDataAgent (used for DFC query)

* Example with Input Files list

  ::

    from DIRAC.TransformationSystem.Client.Transformation import Transformation
    from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
    from DIRAC.Interfaces.API.Job import Job
    j = myJob()
    ...
    t = Transformation( )
    tc = TransformationClient( )
    t.setTransformationName("Reprocessing_1") # This must be unique
    t.setType("DataReprocessing")
    t.setDescription("repro example")
    t.setLongDescription( "This is the long description of my reprocessing" ) #mandatory
    t.setBody ( j.workflow.toXML() )
    t.addTransformation() #transformation is created here
    t.setStatus("Active")
    t.setAgentType("Automatic")
    transID = t.getTransformationID()
    tc.addFilesToTransformation(transID['Value'],infileList) # Files are added here


* Example with Input Files as a result of a DFC query.
  Just replace the above example with a DFC query (example taken from CTA):

  ::

    tc.createTransformationInputDataQuery(transID['Value'], {'particle': 'proton','prodName':'ConfigtestCorsika','outputType':'corsikaData'})

**Note:**

  * *Transformation Type* = 'DataReprocessing'
  * If the 'MonitorFiles' option is enabled in the agent configuration, failed jobs are automatically rescheduled

Data management transformations
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Generation of bulk data removal/replication requests from a fixed file list or as a result of a DFC query

* **Agents**

  ::

    TransformationAgent, RequestTaskAgent, InputDataAgent (for DFC query)

  Requests are then treated by the RMS (see `RequestManagement <http://diracgrid.org/files/docs/AdministratorGuide/Systems/RequestManagement/rms.html>`_):

  * Check the logs of RequestExecutingAgent, *e.g.*:

    ::

      2014-07-08 08:27:33 UTC RequestManagement/RequestExecutingAgent/00000188_00000001   INFO: request '00000188_00000001' is done

  * Query the ReqDB to check the requests

* Example of data removal

  ::

    from DIRAC.TransformationSystem.Client.Transformation import Transformation
    from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient

    infileList = []
    ...

    t = Transformation( )
    tc = TransformationClient( )
    t.setTransformationName("DM_Removal") # Must be unique
    #t.setTransformationGroup("Group1")
    t.setType("Removal")
    t.setPlugin("Standard") # Not needed. The default is 'Standard'
    t.setDescription("dataset1 Removal")
    t.setLongDescription( "Long description of dataset1 Removal" ) # Mandatory
    t.setGroupSize(2) # Here you specify how many files should be grouped within the same request, e.g. 100
    t.setBody ( "Removal;RemoveFile" ) # Mandatory (the default is a ReplicateAndRegister operation)
    t.addTransformation() # Transformation is created here
    t.setStatus("Active")
    t.setAgentType("Automatic")
    transID = t.getTransformationID()
    tc.addFilesToTransformation(transID['Value'],infileList) # Files are added here

**Note:**

  * It's not needed to set a Plugin, the default is 'Standard'
  * It's mandatory to set the Body, otherwise the default operation is 'ReplicateAndRegister'
  * It's not needed to set a SourceSE nor a TargetSE
  * This script remove all replicas of each file. We should verify how to remove only a subset of replicas (SourceSE?)
  * If you add non existing files to a Transformation, you won't get any particular status, the Transformation just does not progress


* Example for Multiple Operations


  .. code:: python

    from DIRAC.TransformationSystem.Client.Transformation import Transformation
    from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient

    infileList = []
    ...

    t = Transformation( )
    tc = TransformationClient( )
    t.setTransformationName("DM_Moving") # Must be unique
    #t.setTransformationGroup("Moving")
    t.setType("Moving")
    t.setPlugin("Standard") # Not needed. The default is 'Standard'
    t.setDescription("dataset1 Moving")
    t.setLongDescription( "Long description of dataset1 Moving" ) # Mandatory
    t.setGroupSize(2) # Here you specify how many files should be grouped within he same request, e.g. 100

    transBody = [ ( "ReplicateAndRegister", { "SourceSE":"FOO-SRM", "TargetSE":"BAR-SRM" }),
                  ( "RemoveReplica", { "TargetSE":"FOO-SRM" } ),
                ]

    t.setBody ( transBody ) # Mandatory
    t.addTransformation() # Transformation is created here
    t.setStatus("Active")
    t.setAgentType("Automatic")
    transID = t.getTransformationID()
    tc.addFilesToTransformation(transID['Value'],infileList) # Files are added here



Data replication based on Catalog Query
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* Example of data replication (file list as a result of a DFC query, example taken from CTA)

  ::

    from DIRAC.TransformationSystem.Client.Transformation import Transformation
    from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
    t = Transformation( )
    tc = TransformationClient( )
    t.setTransformationName("DM_ReplicationByQuery1") # This must vary
    #t.setTransformationGroup("Group1")
    t.setType("Replication")
    t.setSourceSE(['CYF-STORM-Disk','DESY-ZN-Disk']) # A list of SE where at least 1 SE is the valid one
    t.setTargetSE(['CEA-Disk'])
    t.setDescription("data Replication")
    t.setLongDescription( "data Replication" ) #mandatory
    t.setGroupSize(1)
    t.setPlugin("Broadcast")
    t.addTransformation() #transformation is created here
    t.setStatus("Active")
    t.setAgentType("Automatic")
    transID = t.getTransformationID()
    tc.createTransformationInputDataQuery(transID['Value'], {'particle': 'gamma','prodName':'Config_test300113','outputType':'Data','simtelArrayProdVersion':'prod-2_21122012_simtel','runNumSeries':'0'}) # Add files to Transformation based on Catalog Query

--------------------------
Actions on transformations
--------------------------

* **Start**
* **Stop**
* **Flush:** It has a meaning only depending on the plugin used, for example the 'BySize' plugin, used *e.g.* for merging productions, creates a task if there are enough files in input to have at least a certain size: 'flush' will make the 'BySize' plugin to ignore such requirement
* **Complete:** The transformation can be archived by the TransformationCleaningAgent. Archived means that the data produced stay, but not the entries in the TransformationDB
* **Clean:** The transformation is cleaned by the TransformationCleaningAgent: jobs are killed and removed from WMS. Produced and stored files are removed from the Storage Elements, when "OutputDirectories" parameter is set for the transformation.
