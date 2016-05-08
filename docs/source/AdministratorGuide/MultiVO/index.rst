==============
Multi-VO DIRAC
==============

:author:  Bruno Santeramo <bruno.santeramo@ba.infn.it>
:date:    3rd May 2013
:version: 1.0


In this chapter a guide to install and configure DIRAC for multi-VO usage.

.. toctree::
   :maxdepth: 1
   :numbered:

.. contents:: Table of contents
   :depth: 4

--------------------------------------
Before to start with this tutorial ...
--------------------------------------

In this tutorial
 - Server hostname is: dirac.ba.infn.it
 - first VO configured is: superbvo.org
 - second VO configured is: pamela
 - adding more VOs can be done following instructions for the second one
 - foreach VO a <vo_name>_user group is configured to allow normal user operations
 - foreach VO a Pool_<vo_name> submit pool is configured

Limits of this guide
 - This guide must be considered as a step-by-step tutorial, not intended as documentation for DIRAC's multi-VO capabilities.
 - Please, feel free to send me via email any suggestion to improve this chapter.

-------------------------
DIRAC server installation
-------------------------

First step is to install DIRAC. Procedure is the same for a single VO installation, but avoiding VirtualOrganization parameter in configuration file:
::

   ...
   #  VO name (not mandatory, useful if DIRAC will be used for a VO)
   #VirtualOrganization = superbvo.org
   ...


-------------------------
DIRAC client installation
-------------------------

Second step is to install a dirac client and configure it for new installation.

----------------------------------------
Configuring first VO (e.g. superbvo.org)
----------------------------------------

Registry
--------

Add superb_user group
::

   Registry
   {
     DefaultGroup = superb_user, user
   }

Registry/VO
-----------
::

   Registry
   {
     VO
     {
       superbvo.org
       {
         SubmitPools = Pool_superbvo.org
         VOAdmin = bsanteramo
         VOMSName = superbvo.org
         VOMSServers
         {
           voms2.cnaf.infn.it
           {
             DN = /C=IT/O=INFN/OU=Host/L=CNAF/CN=voms2.cnaf.infn.it
             CA = /C=IT/O=INFN/CN=INFN CA
             Port = 15009
           }
           voms-02.pd.infn.it
           {
             DN = /C=IT/O=INFN/OU=Host/L=Padova/CN=voms-02.pd.infn.it
             CA = /C=IT/O=INFN/CN=INFN CA
             Port = 15009
           }
         }
       }
     }
   }

Registry/Groups
---------------
::

   Registry
   {
     Groups
     {
       superb_user
       {
         Users = bsanteramo
         Properties = NormalUser
         VOMSRole = /superbvo.org
         VOMSVO = superbvo.org
         VO = superbvo.org
         SubmitPool = Pool_superbvo.org
         AutoAddVOMS = True
         AutoUploadProxy = True
         AutoUploadPilotProxy = True
       }
     }
   }

Registry/VOMS
-------------
::

   Registry
   {
     VOMS
     {
       Mapping
       {
         superb_user = /superbvo.org
       }
       Servers
       {
         superbvo.org
         {
           voms2.cnaf.infn.it
           {
             DN = /C=IT/O=INFN/OU=Host/L=CNAF/CN=voms2.cnaf.infn.it
             CA = /C=IT/O=INFN/CN=INFN CA
             Port = 15009
           }
           voms-02.pd.infn.it
           {
             DN = /C=IT/O=INFN/OU=Host/L=Padova/CN=voms-02.pd.infn.it
             CA = /C=IT/O=INFN/CN=INFN CA
             Port = 15009
           }
         }
       }
     }
   }

$HOME/.glite/vomses
-------------------

DIRAC search for VOMS data in ``$HOME/.glite/vomses`` folder.
For each VO create a file with the same name of VO and fill 
it in this way for every VOMS server.
(Take data from http://operations-portal.egi.eu/vo)
::

   "<VO name>" "<VOMS server>" "<vomses port>" "<DN>" "<VO name>" "<https port>"

For example::

   [managai@dirac vomses]$ cat /usr/etc/vomses/superbvo.org 
   "superbvo.org" "voms2.cnaf.infn.it" "15009" "/C=IT/O=INFN/OU=Host/L=CNAF/CN=voms2.cnaf.infn.it" "superbvo.org" "8443"
   "superbvo.org" "voms-02.pd.infn.it" "15009" "/C=IT/O=INFN/OU=Host/L=Padova/CN=voms-02.pd.infn.it" "superbvo.org" "8443"


Systems/Configuration - CE2CSAgent
----------------------------------

CE2CSAgent retrieve CE info from BDII. For each VO should 
be an instance of the CE2CSAgent::

   Systems
   {
     Configuration
     {
       Production
       {
         Agents
         {
           CE2CSAgent
           {
             BannedCSs = 
             MailTo = 
             MailFrom = 
             VirtualOrganization = superbvo.org
           }
         }
       }
     }
   }

Operations - Shifter
--------------------
::

   Operations
   {
     SuperB-Production
     {
       Shifter
       {
         SAMManager
         {
           User = bsanteramo
           Group = superb_user
         }
         ProductionManager
         {
           User = bsanteramo
           Group = superb_user
         }
         DataManager
         {
           User = bsanteramo
           Group = superb_user
         }
       }
     }
   }

Operations/JobDescription
-------------------------

Add new Pool to SubmitPools
::

   Operations
   {
     JobDescription
     {
       AllowedJobTypes = MPI
       AllowedJobTypes += User
       AllowedJobTypes += Test
       SubmitPools = Pool_superbvo.org
     }
   }

Resources/FileCatalog
---------------------

Configure DIRAC File Catalog (DFC)
::

   Resources
   {
     FileCatalogs
     {
       FileCatalog
       {
         AccessType = Read-Write
         Status = Active
         Master = True
       }
     }
   }

Resources/StorageElements/ProductionSandboxSE
---------------------------------------------
::

   Resources
   {
     StorageElements
     {
       ProductionSandboxSE
       {
         BackendType = DISET
         AccessProtocol.1
         {
           Host = dirac.ba.infn.it
           Port = 9196
           ProtocolName = DIP
           Protocol = dips
           Path = /WorkloadManagement/SandboxStore
           Access = remote
         }
       }
     }
   }

WorkloadManagement - PilotStatusAgent
-------------------------------------

Option value could be different, it depends on UI 
installed on server
::

   Systems/WorkloadManagement/<setup>/Agents/PilotStatusAgent/GridEnv = /etc/profile.d/grid-env

Systems/WorkloadManagement - TaskQueueDirector
----------------------------------------------
::

   Systems
   {
     WorkloadManagement
     {
       Production
       {
         Agents
         {
           TaskQueueDirector
           {
             DIRACVersion = v6r11p1
             Status = Active
             ListMatchDelay = 10
             extraPilotFraction = 1.0
             extraPilots = 2
             pilotsPerIteration = 100
             maxThreadsInPool = 8
             PollingTime = 30
             MaxCycles = 500
             SubmitPools = Pool_superbvo.org
             AllowedSubmitPools = Pool_superbvo.org
             Pool_superbvo.org
             {
               GridMiddleware = gLite
               ResourceBrokers = wms-multi.grid.cnaf.infn.it
               Failing = 
               PrivatePilotFraction = 1.0
               MaxJobsInFillMode = 5
               Rank = ( other.GlueCEStateWaitingJobs == 0 ? ( other.GlueCEStateFreeCPUs * 10 / other.GlueCEInfoTotalCPUs + other.GlueCEInfoTotalCPUs / 500 ) : -other.GlueCEStateWaitingJobs * 4 / (other.GlueCEStateRunningJobs + 1 ) - 1 )
               GenericPilotDN = /C=IT/O=INFN/OU=Personal Certificate/L=Bari/CN=Bruno Santeramo
               GenericPilotGroup = superb_user
               GridEnv = /etc/profile.d/grid-env
               VirtualOrganization = superbvo.org
             }
             DIRAC
             {
               GridMiddleware = DIRAC
             }
           }
         }
       }
     }
   }

DONE
----

First VO configuration finished... Upload shifter certificates, 
add some CE and test job submission works properly 
(webportal Job Launchpad is useful for testing purpose)

------------------------------------
Configuring another VO (e.g. pamela)
------------------------------------

$HOME/.glite/vomses
-------------------
Add the other VO following the same convention as above.


Registry
--------
::

   Registry
   {
     DefaultGroup = pamela_user, superb_user, user
   }

Registry/VO
-----------

Add pamela
::

   Registry
   {
     VO
     {
       pamela
       {
         SubmitPools = Pool_pamela
         VOAdmin = bsanteramo
         VOMSName = pamela
         VOMSServers
         {
           voms.cnaf.infn.it
           {
             DN = /C=IT/O=INFN/OU=Host/L=CNAF/CN=voms.cnaf.infn.it
             CA = /C=IT/O=INFN/CN=INFN CA
             Port = 15013
           }
           voms-01.pd.infn.it
           {
             DN = /C=IT/O=INFN/OU=Host/L=Padova/CN=voms-01.pd.infn.it
             CA = /C=IT/O=INFN/CN=INFN CA
             Port = 15013
           }
         }
       }
     }
   }

Registry/Groups
---------------

Add pamela_user
::

   Registry
   {
     Groups
     {
       pamela_user
       {
         Users = bsanteramo
         Properties = NormalUser
         VOMSRole = /pamela
         VOMSVO = pamela
         VO = pamela
         SubmitPool = Pool_pamela
         AutoAddVOMS = True
         AutoUploadProxy = True
         AutoUploadPilotProxy = True
       }
     }
   }

Registry/VOMS
-------------

Add pamela parameters...
::

   Registry
   {
     VOMS
     {
       Mapping
       {
         pamela_user = /pamela
       }
       Servers
       {
         pamela
         {
           voms.cnaf.infn.it
           {
             DN = /C=IT/O=INFN/OU=Host/L=CNAF/CN=voms.cnaf.infn.it
             CA = /C=IT/O=INFN/CN=INFN CA
             Port = 15013
           }
           voms-01.pd.infn.it
           {
             DN = /C=IT/O=INFN/OU=Host/L=Padova/CN=voms-01.pd.infn.it
             CA = /C=IT/O=INFN/CN=INFN CA
             Port = 15013
           }
         }
       }
     }
   }

Systems/Configuration - CE2CSAgent
----------------------------------
::

   Systems
   {
     Configuration
     {
       Production
       {
         Agents
         {
           CE2CSAgent
           {
             PollingTime = 86400
             Status = Active
             MaxCycles = 500
             LogLevel = INFO
             BannedCSs = 
             MailTo = 
             MailFrom = 
             VirtualOrganization = superbvo.org
           }
           CE2CSAgent_pamela
           {
             Module = CE2CSAgent
             #This parameter overwrites the default value
             VirtualOrganization = pamela
           }
         }
       }
     }
   }

As dirac_admin group member, enter dirac-admin-sysadmin-cli
::

   (dirac.ba.infn.it)> install agent Configuration CE2CSAgent_pamela -m CE2CSAgent -p VirtualOrganization=pamela
   agent Configuration_CE2CSAgent_pamela is installed, runit status: Run

Operations - adding pamela section
----------------------------------
::

   Operations
   {
     EMail
     {
       Production = bruno.santeramo@ba.infn.it
       Logging = bruno.santeramo@ba.infn.it
     }
     SuperB-Production
     {
       Shifter
       {
         SAMManager
         {
           User = bsanteramo
           Group = superb_user
         }
         ProductionManager
         {
           User = bsanteramo
           Group = superb_user
         }
         DataManager
         {
           User = bsanteramo
           Group = superb_user
         }
       }
     }
     JobDescription
     {
       AllowedJobTypes = MPI
       AllowedJobTypes += User
       AllowedJobTypes += Test
       SubmitPools = Pool_superbvo.org
       SubmitPools += Pool_pamela
     }
     pamela
     {
       SuperB-Production
       {
         Shifter
         {
           SAMManager
           {
             User = bsanteramo
             Group = pamela_user
           }
           ProductionManager
           {
             User = bsanteramo
             Group = pamela_user
           }
           DataManager
           {
             User = bsanteramo
             Group = pamela_user
           }
         }
       }
     }
   }

Systems/WorkloadManagement - TaskQueueDirector
----------------------------------------------
::

   Systems
   {
     WorkloadManagement
     {
       Production
       {
         Agents
         {
           TaskQueueDirector
           {
             DIRACVersion = v6r11p1
             Status = Active
             ListMatchDelay = 10
             extraPilotFraction = 1.0
             extraPilots = 2
             pilotsPerIteration = 100
             maxThreadsInPool = 8
             PollingTime = 30
             MaxCycles = 500
             SubmitPools = Pool_superbvo.org
             SubmitPools += Pool_pamela
             AllowedSubmitPools = Pool_superbvo.org
             AllowedSubmitPools += Pool_pamela
             Pool_superbvo.org
             {
               GridMiddleware = gLite
               ResourceBrokers = wms-multi.grid.cnaf.infn.it
               Failing = 
               PrivatePilotFraction = 1.0
               MaxJobsInFillMode = 5
               Rank = ( other.GlueCEStateWaitingJobs == 0 ? ( other.GlueCEStateFreeCPUs * 10 / other.GlueCEInfoTotalCPUs + other.GlueCEInfoTotalCPUs / 500 ) : -other.GlueCEStateWaitingJobs * 4 / (other.GlueCEStateRunningJobs + 1 ) - 1 )
               GenericPilotDN = /C=IT/O=INFN/OU=Personal Certificate/L=Bari/CN=Bruno Santeramo
               GenericPilotGroup = superb_user
               GridEnv = /etc/profile.d/grid-env
               VirtualOrganization = superbvo.org
             }
             Pool_pamela
             {
               GridMiddleware = gLite
               ResourceBrokers = wms-multi.grid.cnaf.infn.it
               Failing = 
               PrivatePilotFraction = 1.0
               MaxJobsInFillMode = 5
               Rank = ( other.GlueCEStateWaitingJobs == 0 ? ( other.GlueCEStateFreeCPUs * 10 / other.GlueCEInfoTotalCPUs + other.GlueCEInfoTotalCPUs / 500 ) : -other.GlueCEStateWaitingJobs * 4 / (other.GlueCEStateRunningJobs + 1 ) - 1 )
               GenericPilotDN = /C=IT/O=INFN/OU=Personal Certificate/L=Bari/CN=Bruno Santeramo
               GenericPilotGroup = pamela_user
               GridEnv = /etc/profile.d/grid-env
               VirtualOrganization = pamela
             }
             DIRAC
             {
               GridMiddleware = DIRAC
             }
           }
         }
       }
     }
   }
