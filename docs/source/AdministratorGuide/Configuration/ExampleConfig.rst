
==========================
Full Configuration Example
==========================

.. This file is created by docs/Tools/UpdateDiracCFG.py

Below is a complete example configuration with anotations for some sections::

  Systems
  {
    #the systems section is automatically obtained from the ConfigTemplate.cfg files and can be found at
    #https://dirac.readthedocs.org/en/latest/AdministratorGuide/Configuration/ExampleConfig.html
    DataManagementSystem
    {
      Agents
      {
        #http://dirac.readthedocs.io/en/latest/AdministratorGuide/Systems/DataManagement/fts.html#cleanftsdbagent
        #Used to clean the database
        CleanFTSDBAgent
        {
          DeleteGraceDays = 21 # time after which deleting a job in a final status
          DeleteLimitPerCycle = 100 # max number of jobs to delete per agent cycle
        }
        #http://dirac.readthedocs.io/en/latest/AdministratorGuide/Systems/DataManagement/fts.html#ftsagent
        #Agent to perform the fts transfers
        FTSAgent
        {
          MaxFilesPerJob = 10 # maximum number of files in a single fts job
          MaxRequests = 1000 # maximum number of requests to look at per agent's cycle
          MaxThreads = 60 # maximum number of threads
          MaxTransferAttempts = 256 # maximum number of time we attempt to transfer a file
          MinThreads = 10 # minimum number of threads
          MonitoringInterval = 1800 # interval between two monitoring of an FTSJob in second
          PinTime = 18000 # when staging
          PinTime += pin time requested in the FTS job in second
          ProcessJobRequests = True # if this agent is meant to process job only transfers (see `http://dirac.readthedocs.io/en/latest/AdministratorGuide/Systems/DataManagement/fts.html#multiple-ftsagents)
        }
        #http://dirac.readthedocs.io/en/latest/AdministratorGuide/Systems/DataManagement/fts3.html#fts3agent
        FTS3Agent
        {
          OperationBulkSize = 20 # How many Operation we will treat in one loop
          JobBulkSize = 20 # How many Job we will monitor in one loop
          MaxFilesPerJob = 100 # Max number of files to go in a single job
          MaxAttemptsPerFile = 256 # Max number of attempt per file
          DeleteGraceDays = 180 # days before removing jobs
          DeleteLimitPerCycle = 100 # Max number of deletes per cycle
          KickAssignedHours = 1 # hours before kicking jobs with old assignment tag
          KickLimitPerCycle = 100 # Max number of kicks per cycle
        }
      }
      Services
      {
        #http://dirac.readthedocs.io/en/latest/AdministratorGuide/Systems/DataManagement/dfc.html#filecataloghandler
        FileCatalogHandler
        {
          Port = 9197
          DatasetManager = DatasetManager
          DefaultUmask = 0775
          DirectoryManager = DirectoryLevelTree
          DirectoryMetadata = DirectoryMetadata
          FileManager = FileManager
          FileMetadata = FileMetadata
          GlobalReadAccess = True
          LFNPFNConvention = Strong
          ResolvePFN = True
          SecurityManager = NoSecurityManager
          SEManager = SEManagerDB
          UniqueGUID = False
          UserGroupManager = UserAndGroupManagerDB
          ValidFileStatus = [AprioriGoodTrashRemovingProbing]
          ValidReplicaStatus = [AprioriGoodTrashRemovingProbing]
          VisibleFileStatus = [AprioriGood]
          VisibleReplicaStatus = [AprioriGood]
        }
        #http://dirac.readthedocs.io/en/latest/AdministratorGuide/Systems/DataManagement/fts.html#ftsmanager
        FTSManagerHandler
        {
          #No specific configuration
          Port = 9191
        }
        #http://dirac.readthedocs.io/en/latest/AdministratorGuide/Systems/DataManagement/fts.html#ftsmanager
        FTS3ManagerHandler
        {
          #No specific configuration
          Port = 9193
        }
      }
      Databases
      {
        #http://dirac.readthedocs.io/en/latest/AdministratorGuide/Systems/DataManagement/dfc.html#filecatalogdb
        FileCatalogDB
        {
          #No specific configuration
          DBName = FileCatalogDB
        }
        #http://dirac.readthedocs.io/en/latest/AdministratorGuide/Systems/DataManagement/fts.html#ftsdb
        FTSDB
        {
          #No specific configuration
          DBName = FTSDB
        }
        FTS3DB
        {
          #No specific configuration
          DBName = FTS3DB
        }
      }
    }
    RequestManagementSystem
    {
      Agents
      {
        #http://dirac.readthedocs.io/en/latest/AdministratorGuide/Systems/RequestManagement/rmsComponents.html#cleanreqdbagent
        CleanReqDBAgent
        {
          DeleteGraceDays = 60 # Delay after which Requests are removed
          DeleteLimit = 100 # Maximum number of Requests to remove per cycle
          DeleteFailed = False # Whether to delete also Failed request
          KickGraceHours = 1 # After how long we should kick the Requests in `Assigned`
          KickLimit = 10000 # Maximum number of requests kicked by cycle
        }
        #http://dirac.readthedocs.io/en/latest/AdministratorGuide/Systems/RequestManagement/rmsComponents.html#requestexecutingagent
        RequestExecutingAgent
        {
          BulkRequest = 0
          MinProcess = 1
          MaxProcess = 8
          ProcessPoolQueueSize = 25
          ProcessPoolTimeout = 900
          ProcessTaskTimeout = 900
          ProcessPoolSleep = 4
          RequestsPerCycle = 50
          #Define the different Operation types
          #see http://dirac.readthedocs.io/en/latest/AdministratorGuide/Systems/RequestManagement/rmsObjects.html#operation-types
          OperationHandlers
          {
            DummyOperation
            {
              #These parameters can be defined for all handlers
              #The location of the python file, without .py, is mandatory
              Location = DIRAC/DataManagementSystem/Agent/RequestOperations/DummyHandler # Mandatory
              LogLevel = DEBUG # self explanatory
              MaxAttemts = 256 # Maximum attempts per file
              TimeOut = 300 # Timeout in seconds of the operation
              TimeOutPerFile = 40 # Additional delay per file
            }
            ForwardDISET
            {
              Location = DIRAC/RequestManagementSystem/Agent/RequestOperations/ForwardDISET
            }
            MoveReplica
            {
              Location = DIRAC/DataManagementSystem/Agent/RequestOperations/MoveReplica
            }
            PutAndRegister
            {
              Location = DIRAC/DataManagementSystem/Agent/RequestOperations/PutAndRegister
            }
            RegisterFile
            {
              Location = DIRAC/DataManagementSystem/Agent/RequestOperations/RegisterFile
            }
            RegisterReplica
            {
              Location = DIRAC/DataManagementSystem/Agent/RequestOperations/RegisterReplica
            }
            RemoveFile
            {
              Location = DIRAC/DataManagementSystem/Agent/RequestOperations/RemoveFile
            }
            RemoveReplica
            {
              Location = DIRAC/DataManagementSystem/Agent/RequestOperations/RemoveReplica
            }
            ReplicateAndRegister
            {
              Location = DIRAC/DataManagementSystem/Agent/RequestOperations/ReplicateAndRegister
              FTSMode = True # If True
              FTSMode += will use FTS to transfer files
              UseNewFTS3 = True
              FTSBannedGroups = lhcb_user # list of groups for which not to use FTS
            }
            SetFileStatus
            {
              Location = DIRAC/TransformationSystem/Agent/RequestOperations/SetFileStatus
            }
          }
        }
      }
      Databases
      {
        #http://dirac.readthedocs.io/en/latest/AdministratorGuide/Systems/RequestManagement/rmsComponents.html#requestdb
        RequestDB
        {
          #No specific configuration
          DBName = RequestDB
        }
      }
      Services
      {
        #http://dirac.readthedocs.io/en/latest/AdministratorGuide/Systems/RequestManagement/rmsComponents.html#reqmanager
        ReqManager
        {
          Port = 9140
          constantRequestDelay = 0 # Constant delay when retrying a request
        }
        #http://dirac.readthedocs.io/en/latest/AdministratorGuide/Systems/RequestManagement/rmsComponents.html#reqproxy
        ReqProxy
        {
          Port = 9161
        }
      }
      URLs
      {
        #Yes.... it is ReqProxyURLs, and not ReqProxy...
        #http://dirac.readthedocs.io/en/latest/AdministratorGuide/Systems/RequestManagement/rmsComponents.html#reqproxy
        ReqProxyURLs = dips://server1:9161/RequestManagement/ReqProxy
        ReqProxyURLs += dips://server2:9161/RequestManagement/ReqProxy
      }
    }
    TransformationSystem
    {
      Agents
      {
        #BEGIN TransformationCleaningAgent
        TransformationCleaningAgent
        {
          #MetaData key to use to identify output data
          TransfIDMeta = TransformationID
          #Location of the OutputData, if the OutputDirectories parameter is not set for
          #transformations only 'MetadataCatalog has to be used
          DirectoryLocations = TransformationDB
          DirectoryLocations += MetadataCatalog
          #Enable or disable, default enabled
          EnableFlag = True
          #How many days to wait before archiving transformations
          ArchiveAfter = 7
          #Shifter to use for removal operations, default is empty and
          #using the transformation owner for cleanup
          shifterProxy = 
          #Which transformation types to clean
          #If not filled, transformation types are taken from
          #Operations/Transformations/DataManipulation
          #and Operations/Transformations/DataProcessing
          TransformationTypes = 
          #Time between cycles in seconds
          PollingTime = 3600
        }
      }
    }
    Accounting
    {
      Services
      {
        DataStore
        {
          Port = 9133
          Authorization
          {
            Default = authenticated
            compactDB = ServiceAdministrator
            deleteType = ServiceAdministrator
            registerType = ServiceAdministrator
            setBucketsLength = ServiceAdministrator
            regenerateBuckets = ServiceAdministrator
          }
        }
        ReportGenerator
        {
          Port = 9134
          Authorization
          {
            Default = authenticated
            FileTransfer
            {
              Default = authenticated
            }
          }
        }
      }
      Agents
      {
        NetworkAgent
        {
          MaxCycles = 0
          PollingTime = 60
          MessageQueueURI = 
        }
      }
    }
    Configuration
    {
      Services
      {
        Server
        {
          HandlerPath = DIRAC/ConfigurationSystem/Service/ConfigurationHandler.py
          Port = 9135
          Authorization
          {
            Default = authenticated
            commitNewData = CSAdministrator
            rollbackToVersion = CSAdministrator
            getVersionContents = ServiceAdministrator
            getVersionContents += CSAdministrator
          }
        }
      }
      Agents
      {
        Bdii2CSAgent
        {
          BannedCEs = 
          BannedSEs = 
          SelectedSites = 
          ProcessCEs = yes
          ProcessSEs = no
          MailTo = 
          MailFrom = 
          VirtualOrganization = 
          DryRun = True
          Host = lcg-bdii.cern.ch:2170
          GLUE2URLs = 
          GLUE2Only = False
        }
        VOMS2CSAgent
        {
          PollingTime = 14400
          mailFrom = noreply@dirac.system
          AutoAddUsers = False
          AutoModifyUsers = False
          AutoDeleteUsers = False
          DetailedReport = True
          MakeHomeDirectory = False
          VO = Any
          DryRun = True
        }
        GOCDB2CSAgent
        {
          Cycles = 0
          PollingTime = 14400
          DryRun = True
        }
      }
    }
    DataManagement
    {
      Services
      {
        DataIntegrity
        {
          Port = 9150
          Authorization
          {
            Default = authenticated
          }
        }
        FTSManager
        {
          Port = 9191
          Authorization
          {
            Default = authenticated
          }
        }
        FTS3Manager
        {
          Port = 9193
          Authorization
          {
            Default = authenticated
          }
        }
        FileCatalogProxy
        {
          Port = 9138
          Authorization
          {
            Default = authenticated
          }
        }
        FileCatalog
        {
          Port = 9197
          UserGroupManager = UserAndGroupManagerDB
          SEManager = SEManagerDB
          SecurityManager = NoSecurityManager
          DirectoryManager = DirectoryLevelTree
          FileManager = FileManager
          UniqueGUID = False
          GlobalReadAccess = True
          LFNPFNConvention = Strong
          ResolvePFN = True
          DefaultUmask = 509
          VisibleStatus = AprioriGood
          Authorization
          {
            Default = authenticated
          }
        }
        StorageElement
        {
          BasePath = storageElement
          Port = 9148
          MaxStorageSize = 5000
          Authorization
          {
            Default = authenticated
            FileTransfer
            {
              Default = authenticated
            }
          }
        }
        StorageElementProxy
        {
          BasePath = storageElement
          Port = 9139
          Authorization
          {
            Default = authenticated
            FileTransfer
            {
              Default = authenticated
            }
          }
        }
        IRODSStorageElement
        {
          Port = 9188
          Authorization
          {
            Default = authenticated
            FileTransfer
            {
              Default = authenticated
            }
          }
        }
      }
      Agents
      {
        FTSAgent
        {
          PollingTime = 120
          UseProxies = True
          ControlDirectory = control/DataManagement/FTSAgent
          MinThreads = 1
          MaxThreads = 10
          FTSPlacementValidityPeriod = 600
          StageFiles = True
          MaxFilesPerJob = 100
          MaxTransferAttempts = 256
          shifterProxy = DataManager
        }
        #BEGIN FTS3Agent
        FTS3Agent
        {
          PollingTime = 120
          MaxThreads = 10
          #How many Operation we will treat in one loop
          OperationBulkSize = 20
          #How many Job we will monitor in one loop
          JobBulkSize = 20
          #Max number of files to go in a single job
          MaxFilesPerJob = 100
          #Max number of attempt per file
          MaxAttemptsPerFile = 256
          #days before removing jobs
          DeleteGraceDays = 180
          #Max number of deletes per cycle
          DeleteLimitPerCycle = 100
          #hours before kicking jobs with old assignment tag
          KickAssignedHours = 1
          #Max number of kicks per cycle
          KickLimitPerCycle = 100
        }
        #END
        CleanFTSDBAgent
        {
          PollingTime = 300
          ControlDirectory = control/DataManagement/CleanFTSDBAgent
          DeleteGraceDays = 180
          DeleteLimitPerCycle = 100
          KickAssignedHours = 1
          KickLimitPerCycle = 100
        }
      }
    }
    Framework
    {
      Services
      {
        Gateway
        {
          Port = 9159
        }
        SystemAdministrator
        {
          Port = 9162
          Authorization
          {
            Default = ServiceAdministrator
            storeHostInfo = Operator
          }
        }
        ProxyManager
        {
          Port = 9152
          MaxThreads = 100
          getVOMSProxyWithTokenMaxThreads = 2
          Authorization
          {
            Default = authenticated
            getProxy = FullDelegation
            getProxy += LimitedDelegation
            getProxy += PrivateLimitedDelegation
            getVOMSProxy = FullDelegation
            getVOMSProxy += LimitedDelegation
            getVOMSProxy += PrivateLimitedDelegation
            getProxyWithToken = FullDelegation
            getProxyWithToken += LimitedDelegation
            getProxyWithToken += PrivateLimitedDelegation
            getVOMSProxyWithToken = FullDelegation
            getVOMSProxyWithToken += LimitedDelegation
            getVOMSProxyWithToken += PrivateLimitedDelegation
            getLogContents = ProxyManagement
            setPersistency = ProxyManagement
          }
        }
        SecurityLogging
        {
          Port = 9153
          Authorization
          {
            Default = authenticated
          }
        }
        UserProfileManager
        {
          Port = 9155
          Authorization
          {
            Default = authenticated
          }
        }
        Plotting
        {
          Port = 9157
          PlotsLocation = data/plots
          Authorization
          {
            Default = authenticated
            FileTransfer
            {
              Default = authenticated
            }
          }
        }
        BundleDelivery
        {
          Port = 9158
          Authorization
          {
            Default = authenticated
            FileTransfer
            {
              Default = authenticated
            }
          }
        }
        SystemLogging
        {
          Port = 9141
          Authorization
          {
            Default = authenticated
          }
        }
        SystemLoggingReport
        {
          Port = 9144
          Authorization
          {
            Default = authenticated
          }
        }
        Monitoring
        {
          Port = 9142
          Authorization
          {
            Default = authenticated
            FileTransfer
            {
              Default = authenticated
            }
            queryField = ServiceAdministrator
            tryView = ServiceAdministrator
            saveView = ServiceAdministrator
            deleteView = ServiceAdministrator
            deleteActivity = ServiceAdministrator
            deleteActivities = ServiceAdministrator
            deleteViews = ServiceAdministrator
          }
        }
        Notification
        {
          Port = 9154
          SMSSwitch = sms.switch.ch
          Authorization
          {
            Default = AlarmsManagement
            sendMail = authenticated
            sendSMS = authenticated
            removeNotificationsForUser = authenticated
            markNotificationsAsRead = authenticated
            getNotifications = authenticated
            ping = authenticated
          }
        }
        ComponentMonitoring
        {
          Port = 9190
          Authorization
          {
            Default = ServiceAdministrator
            componentExists = authenticated
            getComponents = authenticated
            hostExists = authenticated
            getHosts = authenticated
            installationExists = authenticated
            getInstallations = authenticated
            updateLog = Operator
          }
        }
        RabbitMQSync
        {
          Port = 9192
          Authorization
          {
            Default = Operator
          }
        }
      }
      Agents
      {
        MyProxyRenewalAgent
        {
          PollingTime = 1800
          MinValidity = 10000
          #The period for which the proxy will be extended. The value is in hours
          ValidityPeriod = 15
        }
        CAUpdateAgent
        {
          PollingTime = 21600
        }
        ErrorMessageMonitor
        {
          Reviewer = 
        }
        SystemLoggingDBCleaner
        {
          RemoveDate = 30
        }
        TopErrorMessagesReporter
        {
          MailList = 
          Reviewer = 
          Threshold = 10
          QueryPeriod = 7
          NumberOfErrors = 10
        }
      }
    }
    Monitoring
    {
      Services
      {
        Monitoring
        {
          Port = 9137
          Authorization
          {
            Default = authenticated
            FileTransfer
            {
              Default = authenticated
            }
          }
        }
      }
    }
    Production
    {
      Services
      {
        ProductionManager
        {
          Port = 9180
          HandlerPath = DIRAC/ProductionSystem/Service/ProductionManagerHandler.py
          Authorization
          {
            Default = authenticated
          }
        }
      }
    }
    RequestManagement
    {
      Services
      {
        ReqManager
        {
          Port = 9140
          Authorization
          {
            Default = authenticated
          }
        }
        ReqProxy
        {
          Port = 9161
          Authorization
          {
            Default = authenticated
          }
        }
      }
      Agents
      {
        RequestExecutingAgent
        {
          PollingTime = 60
          RequestsPerCycle = 50
          MinProcess = 1
          MaxProcess = 8
          ProcessPoolQueueSize = 25
          ProcessPoolTimeout = 900
          ProcessTaskTimeout = 900
          ProcessPoolSleep = 4
          #TimeOut = 300
          #TimeOutPerFile = 300
          MaxAttempts = 256
          BulkRequest = 0
          OperationHandlers
          {
            ForwardDISET
            {
              Location = DIRAC/RequestManagementSystem/Agent/RequestOperations/ForwardDISET
              LogLevel = INFO
              MaxAttempts = 256
              TimeOut = 120
            }
            ReplicateAndRegister
            {
              Location = DIRAC/DataManagementSystem/Agent/RequestOperations/ReplicateAndRegister
              FTSMode = False
              UseNewFTS3 = True
              FTSBannedGroups = dirac_user
              FTSBannedGroups += lhcb_user
              LogLevel = INFO
              MaxAttempts = 256
              TimeOutPerFile = 600
            }
            PutAndRegister
            {
              Location = DIRAC/DataManagementSystem/Agent/RequestOperations/PutAndRegister
              LogLevel = INFO
              MaxAttempts = 256
              TimeOutPerFile = 600
            }
            RegisterReplica
            {
              Location = DIRAC/DataManagementSystem/Agent/RequestOperations/RegisterReplica
              LogLevel = INFO
              MaxAttempts = 256
              TimeOutPerFile = 120
            }
            RemoveReplica
            {
              Location = DIRAC/DataManagementSystem/Agent/RequestOperations/RemoveReplica
              LogLevel = INFO
              MaxAttempts = 256
              TimeOutPerFile = 120
            }
            RemoveFile
            {
              Location = DIRAC/DataManagementSystem/Agent/RequestOperations/RemoveFile
              LogLevel = INFO
              MaxAttempts = 256
              TimeOutPerFile = 120
            }
            RegisterFile
            {
              Location = DIRAC/DataManagementSystem/Agent/RequestOperations/RegisterFile
              LogLevel = INFO
              MaxAttempts = 256
              TimeOutPerFile = 120
            }
            SetFileStatus
            {
              Location = DIRAC/TransformationSystem/Agent/RequestOperations/SetFileStatus
              LogLevel = INFO
              MaxAttempts = 256
              TimeOutPerFile = 120
            }
          }
        }
        CleanReqDBAgent
        {
          PollingTime = 60
          ControlDirectory = control/RequestManagement/CleanReqDBAgent
          DeleteGraceDays = 30
          DeleteLimit = 100
          DeleteFailed = False
          KickGraceHours = 2
          KickLimit = 100
        }
      }
    }
    ResourceStatus
    {
      Services
      {
        ResourceStatus
        {
          Port = 9160
          Authorization
          {
            Default = SiteManager
            select = all
          }
        }
        ResourceManagement
        {
          Port = 9172
          Authorization
          {
            Default = SiteManager
            select = all
          }
        }
        Publisher
        {
          Port = 9165
          Authorization
          {
            Default = Authenticated
          }
        }
      }
      Agents
      {
        #BEGIN SummarizeLogsAgent
        SummarizeLogsAgent
        {
          #Time between cycles in seconds
          PollingTime = 600
        }
        #END
        #BEGIN ElementInspectorAgent
        ElementInspectorAgent
        {
          #Time between cycles in seconds
          PollingTime = 300
          #Maximum number of threads used by the agent
          maxNumberOfThreads = 15
          #Type of element that this agent will run on (Resource or Site)
          elementType = Resource
        }
        #END
        #BEGIN SiteInspectorAgent
        SiteInspectorAgent
        {
          #Time between cycles in seconds
          PollingTime = 300
          #Maximum number of threads used by the agent
          maxNumberOfThreads = 15
        }
        #END
        #BEGIN CacheFeederAgent
        CacheFeederAgent
        {
          #Time between cycles in seconds
          PollingTime = 900
          #Shifter to use by the commands invoked
          shifterProxy = DataManager
        }
        #END
        #BEGIN TokenAgent
        TokenAgent
        {
          #Time between cycles in seconds
          PollingTime = 3600
          #hours to notify the owner of the token in advance to the token expiration
          notifyHours = 12
          #admin e-mail to where to notify about expiring tokens (on top of existing notifications to tokwn owners)
          adminMail = 
        }
        #END
        #BEGIN EmailAgent
        EmailAgent
        {
          #Time between cycles in seconds
          PollingTime = 1800
        }
      }
    }
    StorageManagement
    {
      Services
      {
        StorageManager
        {
          Port = 9149
          Authorization
          {
            Default = authenticated
          }
        }
      }
      Agents
      {
        StageMonitorAgent
        {
          PollingTime = 120
        }
        StageRequestAgent
        {
          PollingTime = 120
        }
        RequestPreparationAgent
        {
          PollingTime = 120
        }
        RequestFinalizationAgent
        {
          PollingTime = 120
        }
      }
    }
    Transformation
    {
      Services
      {
        TransformationManager
        {
          Port = 9131
          HandlerPath = DIRAC/TransformationSystem/Service/TransformationManagerHandler.py
          Authorization
          {
            Default = authenticated
          }
        }
      }
      Agents
      {
        InputDataAgent
        {
          PollingTime = 120
          FullUpdatePeriod = 86400
          RefreshOnly = False
        }
        MCExtensionAgent
        {
          PollingTime = 120
        }
        RequestTaskAgent
        {
          PollingTime = 120
        }
        TransformationAgent
        {
          PollingTime = 120
        }
        #BEGIN TransformationCleaningAgent
        TransformationCleaningAgent
        {
          #MetaData key to use to identify output data
          TransfIDMeta = TransformationID
          #Location of the OutputData, if the OutputDirectories parameter is not set for
          #transformations only 'MetadataCatalog has to be used
          DirectoryLocations = TransformationDB
          DirectoryLocations += MetadataCatalog
          #Enable or disable, default enabled
          EnableFlag = True
          #How many days to wait before archiving transformations
          ArchiveAfter = 7
          #Shifter to use for removal operations, default is empty and
          #using the transformation owner for cleanup
          shifterProxy = 
          #Which transformation types to clean
          #If not filled, transformation types are taken from
          #Operations/Transformations/DataManipulation
          #and Operations/Transformations/DataProcessing
          TransformationTypes = 
          #Time between cycles in seconds
          PollingTime = 3600
        }
        #END
        ValidateOutputDataAgent
        {
          PollingTime = 120
        }
        #BEGIN WorkflowTaskAgent
        WorkflowTaskAgent
        {
          #Transformation types to be taken into account by the agent
          TransType = MCSimulation
          TransType += DataReconstruction
          TransType += DataStripping
          TransType += MCStripping
          TransType += Merge
          #Task statuses considered transient that should be monitored for updates
          TaskUpdateStatus = Submitted
          TaskUpdateStatus += Received
          TaskUpdateStatus += Waiting
          TaskUpdateStatus += Running
          TaskUpdateStatus += Matched
          TaskUpdateStatus += Completed
          TaskUpdateStatus += Failed
          #Status of transformations for which to monitor tasks
          UpdateTasksStatus = Active
          UpdateTasksStatus += Completing
          UpdateTasksStatus += Stopped
          #Number of tasks to be updated in one call
          TaskUpdateChunkSize = 0
          #Give this option a value if the agent should submit Requests
          SubmitTasks = yes
          #Status of transformations for which to submit jobs to WMS
          SubmitStatus = Active
          SubmitStatus += Completing
          #Number of tasks to submit in one execution cycle per transformation
          TasksPerLoop = 50
          #Use a dedicated proxy to submit jobs to the WMS
          shifterProxy = 
          #Use delegated credentials. Use this instead of the shifterProxy option (New in v6r20p5)
          ShifterCredentials = 
          #Give this option a value if the agent should check Reserved tasks
          CheckReserved = 
          #Give this option a value if the agent should monitor tasks
          MonitorTasks = 
          #Give this option a value if the agent should monitor files
          MonitorFiles = 
          #Status of transformations for which to monitor Files
          UpdateFilesStatus = Active
          UpdateFilesStatus += Completing
          UpdateFilesStatus += Stopped
          #Status of transformations for which to check reserved tasks
          CheckReservedStatus = Active
          CheckReservedStatus += Completing
          CheckReservedStatus += Stopped
          #Location of the transformation plugins
          PluginLocation = DIRAC.TransformationSystem.Client.TaskManagerPlugin
          #maximum number of threads to use in this agent
          maxNumberOfThreads = 15
          #Time between cycles in seconds
          PollingTime = 120
          #Fill in this option if you want to activate bulk submission (for speed up)
          BulkSubmission = false
        }
      }
    }
    WorkloadManagement
    {
      Services
      {
        JobManager
        {
          Port = 9132
          MaxParametricJobs = 100
          Authorization
          {
            Default = authenticated
          }
        }
        JobMonitoring
        {
          Port = 9130
          Authorization
          {
            Default = authenticated
          }
        }
        JobStateUpdate
        {
          Port = 9136
          Authorization
          {
            Default = authenticated
          }
          SSLSessionTime = 86400
          MaxThreads = 100
        }
        #Parameters of the WMS Matcher service
        Matcher
        {
          Port = 9170
          MaxThreads = 20
          #Flag for checking the DIRAC version of the pilot is the current production one as defined
          #in /Operations/<vo>/<setup>/Versions/PilotVersion option
          CheckPilotVersion = Yes
          #Flag to check the site job limits
          SiteJobLimits = False
          Authorization
          {
            Default = authenticated
            getActiveTaskQueues = JobAdministrator
          }
        }
        #Parameters of the WMS Administrator service
        WMSAdministrator
        {
          Port = 9145
          Authorization
          {
            Default = Operator
            getJobPilotOutput = authenticated
            setJobForPilot = authenticated
            setPilotBenchmark = authenticated
            setPilotStatus = authenticated
            getSiteMask = authenticated
            getSiteMaskStatus = authenticated
            ping = authenticated
            getPilots = authenticated
            allowSite = SiteManager
            allowSite += Operator
            banSite = SiteManager
            banSite += Operator
            getPilotSummary = authenticated
            getSiteMaskLogging = authenticated
            getPilotSummaryWeb = authenticated
            getPilotMonitorWeb = authenticated
            getSiteSummaryWeb = authenticated
            getSiteSummarySelectors = authenticated
            getPilotLoggingInfo = authenticated
            getPilotMonitorSelectors = authenticated
          }
        }
        #Parameters of the PilotsLogging service
        PilotsLogging
        {
          Port = 9146
          Authorization
          {
            Default = Operator
            getPilotsLogging = authenticated
            addPilotsLogging = Operator
            deletePilotsLogging = Operator
          }
          Enable = No
          PilotsLoggingQueue = serviceURL::QueueType::QueueName
        }
        SandboxStore
        {
          Port = 9196
          LocalSE = ProductionSandboxSE
          MaxThreads = 200
          toClientMaxThreads = 100
          Backend = local
          MaxSandboxSizeMiB = 10
          SandboxPrefix = Sandbox
          BasePath = /opt/dirac/storage/sandboxes
          DelayedExternalDeletion = True
          Authorization
          {
            Default = authenticated
            FileTransfer
            {
              Default = authenticated
            }
          }
        }
        OptimizationMind
        {
          Port = 9175
        }
      }
      Agents
      {
        PilotStatusAgent
        {
          PollingTime = 300
          #Flag enabling sending of the Pilot accounting info to the Accounting Service
          PilotAccountingEnabled = yes
        }
        JobAgent
        {
          FillingModeFlag = true
          StopOnApplicationFailure = true
          StopAfterFailedMatches = 10
          SubmissionDelay = 10
          CEType = InProcess
          JobWrapperTemplate = DIRAC/WorkloadManagementSystem/JobWrapper/JobWrapperTemplate.py
        }
        StalledJobAgent
        {
          StalledTimeHours = 2
          FailedTimeHours = 6
          PollingTime = 120
        }
        #BEGIN JobCleaningAgent
        JobCleaningAgent
        {
          PollingTime = 3600
          #Maximum number of jobs to be processed in one cycle
          MaxJobsAtOnce = 500
          #Delete jobs individually, if True
          JobByJob = False
          #Seconds to wait between jobs if JobByJob is true
          ThrottlingPeriod = 0.0
          RemoveStatusDelay
          {
            #Number of days after which Done jobs are removed
            Done = 7
            #Number of days after which Killed jobs are removed
            Killed = 7
            #Number of days after which Failed jobs are removed
            Failed = 7
            #Number of days after which any jobs, irrespective of status is removed (-1 for disabling this feature)
            Any = -1
          }
          #Which production type jobs _not_ to remove, takes default from Operations/Transformations/DataProcessing
          ProductionTypes = 
        }
        #END
        #BEGIN SiteDirector
        SiteDirector
        {
          #VO treated (leave empty for auto-discovery)
          VO = 
          #VO treated (leave empty for auto-discovery)
          Community = 
          #Group treated (leave empty for auto-discovery)
          Group = 
          #Grid Environment (leave empty for auto-discovery)
          GridEnv = 
          #Pilot 3 option
          Pilot3 = False
          #the DN of the certificate proxy used to submit pilots. If not found here, what is in Operations/Pilot section of the CS will be used
          PilotDN = 
          #the group of the certificate proxy used to submit pilots. If not found here, what is in Operations/Pilot section of the CS will be used
          PilotGroup = 
          #List of sites that will be treated by this SiteDirector
          Site = any
          #List of CE types that will be treated by this SiteDirector
          CETypes = any
          #List of CEs that will be treated by this SiteDirector
          CEs = any
          #The maximum length of a queue (in seconds). Default: 3 days
          MaxQueueLength = 259200
          #The maximum number of jobs in filling mode
          MaxJobsInFillMode = 5
          #Log level of the pilots
          PilotLogLevel = INFO
          #Max number of pilots to submit per cycle
          MaxPilotsToSubmit = 100
          #Check, or not, for the waiting pilots already submitted
          PilotWaitingFlag = True
          #How many cycels to skip if queue is not working
          FailedQueueCycleFactor = 10
          #Every N cycles we update the pilots status
          PilotStatusUpdateCycleFactor = 10
          #To submit pilots to empty sites in any case
          AddPilotsToEmptySites = False
          #Should the SiteDirector consider platforms when deciding to submit pilots?
          CheckPlatform = False
          #Attribute used to define if the status of the pilots will be updated
          UpdatePilotStatus = True
          #Boolean value used to indicate if the pilot output will be or not retrieved
          GetPilotOutput = False
          #Boolean value than indicates if the pilot job will send information for accounting
          SendPilotAccounting = True
        }
        #END
        MultiProcessorSiteDirector
        {
          PollingTime = 120
          CETypes = CREAM
          Site = Any
          MaxJobsInFillMode = 5
          PilotLogLevel = INFO
          GetPilotOutput = False
          UpdatePilotStatus = True
          SendPilotAccounting = True
          FailedQueueCycleFactor = 10
          PilotStatusUpdateCycleFactor = 10
          AddPilotsToEmptySites = False
        }
        StatesAccountingAgent
        {
          PollingTime = 120
        }
        #BEGIN StatesMonitoringAgent
        StatesMonitoringAgent
        {
          PollingTime = 900
          #the name of the message queue used for the failover
          MessageQueue = dirac.wmshistory
        }
      }
      #END
      Executors
      {
        Optimizers
        {
          Load = JobPath
          Load += JobSanity
          Load += InputData
          Load += JobScheduling
        }
        JobPath
        {
        }
        JobSanity
        {
        }
        InputData
        {
        }
        JobScheduling
        {
        }
      }
    }
  }
  #END
  Resources
  {
    #Where all your Catalogs are defined
    FileCatalogs
    {
      #There is one section per catalog
      #See http://dirac.readthedocs.io/en/latest/AdministratorGuide/Resources/Catalog/index.html
      <MyCatalog>
      {
        CatalogType = <myCatalogType> # used for plugin selection
        CatalogURL = <myCatalogURL> # used for DISET URL
      }
    }
    #FTS endpoint definition http://dirac.readthedocs.io/en/latest/AdministratorGuide/Systems/DataManagement/fts.html#fts-servers-definition
    <anyOptions> # Passed to the constructor of the pluginFTSEndpoints
    {
      FTS3
      {
        CERN-FTS3 = https://fts3.cern.ch:8446
      }
    }
    #Abstract definition of storage elements, used to be inherited.
    #see http://dirac.readthedocs.io/en/latest/AdministratorGuide/Resources/Storage/index.html#storageelementbases
    StorageElementBases
    {
      #The base SE definition can contain all the options of a normal SE
      #http://dirac.readthedocs.io/en/latest/AdministratorGuide/Resources/Storage/index.html#storageelements
      CERN-EOS
      {
        BackendType = eos # backend type of storage element
        SEType = T0D1 # Tape or Disk SE
        UseCatalogURL = True # used the stored url or generate it (default False)
        ReadAccess = True # Allowed for Read if no RSS enabled
        WriteAccess = True # Allowed for Write if no RSS enabled
        CheckAccess = True # Allowed for Check if no RSS enabled
        RemoveAccess = True # Allowed for Remove if no RSS enabled
        #Protocol section, see   http://dirac.readthedocs.io/en/latest/AdministratorGuide/Resources/Storage/index.html#available-protocol-plugins
        GFAL2_SRM2
        {
          Host = srm-eoslhcb.cern.ch
          Port = 8443
          PluginName = GFAL2_SRM2 # If different from the section name
          Protocol = srm # primary protocol
          Path = /eos/lhcb/grid/prod # base path
          Access = remote
          SpaceToken = LHCb-EOS
          WSUrl = /srm/v2/server?SFN=
        }
      }
    }
    #http://dirac.readthedocs.io/en/latest/AdministratorGuide/Resources/Storage/index.html#storageelements
    StorageElements
    {
      #Just inherit everything from CERN-EOS, without change
      CERN-DST-EOS
      {
        BaseSE = CERN-EOS
      }
      CERN-USER # inherit from CERN-EOS
      {
        BaseSE = CERN-EOS
        #Modify the options for Gfal2
        GFAL2_SRM2
        {
          Path = /eos/lhcb/grid/user
          SpaceToken = LHCb_USER
        }
        #Add an extra protocol
        GFAL2_XROOT
        {
          Host = eoslhcb.cern.ch
          Port = 8443
          Protocol = root
          Path = /eos/lhcb/grid/user
          Access = remote
          SpaceToken = LHCb-EOS
          WSUrl = /srm/v2/server?SFN=
        }
      }
      CERN-ALIAS
      {
        Alias = CERN-USER # Use CERN-USER when instanciating CERN-ALIAS
      }
    }
    #See http://dirac.readthedocs.io/en/latest/AdministratorGuide/Resources/Storage/index.html#storageelementgroups
    StorageElementGroups
    {
      CERN-Storages = CERN-DST-EOS
      CERN-Storages += CERN-USER
    }
  }
  Operations
  {
    #This is the default section of operations.
    #Any value here can be overwriten in the setup specific section
    Defaults
    {
      DataManagement
      {
        #see http://dirac.readthedocs.io/en/latest/AdministratorGuide/Resources/Catalog/index.html#multi-protocol
        #for the next 4 options
        AccessProtocols = srm
        AccessProtocols += dips
        RegistrationProtocols = srm
        RegistrationProtocols += dips
        ThirdPartyProtocols = srm
        WriteProtocols = srm
        WriteProtocols += dips
        #FTS related options. See http://dirac.readthedocs.io/en/latest/AdministratorGuide/Systems/DataManagement/fts.html
        FTSVersion = FTS3 # should only be that...
        FTSPlacement
        {
          FTS3
          {
            ServerPolicy = Random # http://dirac.readthedocs.io/en/latest/AdministratorGuide/Systems/DataManagement/fts.html#ftsserver-policy
          }
        }
      }
      Services
      {
        #See http://dirac.readthedocs.io/en/latest/AdministratorGuide/Resources/Catalog/index.html
        Catalogs
        {
          CatalogList = Catalog1
          CatalogList += Catalog2
          CatalogList += etc # List of catalogs defined in Resources to use
          #Each catalog defined in Resources should also contain some runtime options here
          <MyCatalog>
          {
            Status = Active # enable the catalog or not (default Active)
            AccessType = Read-Write # No default
            AccessType += must be set
            Master = True # See http://dirac.readthedocs.io/en/latest/AdministratorGuide/Resources/Catalog/index.html#master-catalog
            #Dynamic conditions to enable or not the catalog
            #See http://dirac.readthedocs.io/en/latest/AdministratorGuide/Resources/Catalog/index.html#conditional-filecatalogs
            Conditions
            {
              WRITE = <myWriteCondition>
              READ = <myReadCondition>
              ALL = <valid for all conditions>
              <myMethod> = <myCondition valid only for myMethod>
            }
          }
        }
      }
    }
    #Options in this section will only be used when running with the
    #<MySetup> setup
    <MySetup>
    {
    }
  }
