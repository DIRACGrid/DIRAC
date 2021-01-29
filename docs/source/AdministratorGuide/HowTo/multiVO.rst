.. _multi_vo_dirac:

==============
Multi-VO DIRAC
==============

:author:  Bruno Santeramo <bruno.santeramo at ba.infn.it> - Federico Stagni (fstagni at cern.ch)
:date:    05/2013 - small update 03/2018
:version: 1.1


In this chapter a guide to install and configure DIRAC for multi-VO usage.

.. toctree::
   :maxdepth: 1
   :numbered:

.. contents:: Table of contents
   :depth: 4

--------------------------------------
Before starting with this tutorial ...
--------------------------------------

In this tutorial
 - Server hostname is: dirac.ba.infn.it
 - first VO configured is: superbvo.org
 - second VO configured is: pamela
 - adding more VOs can be done following instructions for the second one
 - for each VO a <vo_name>_user group is configured to allow normal user operations

Limits to this guide
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
     DefaultGroup = superb_user
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
         }
       }
     }
   }

Registry/Groups
---------------

Here define the users part of the "superb_user" group, its DIRAC properties, and its VOMS properties.
::

   Registry
   {
     Groups
     {
       superb_user
       {
         Users = bsanteramo, anotherUser
         Properties = NormalUser
         VOMSRole = /superbvo.org
         VOMSVO = superbvo.org
         VO = superbvo.org
         AutoAddVOMS = True
         AutoUploadProxy = True
         AutoUploadPilotProxy = True
       }
     }
   }

$HOME/.glite/vomses
-------------------

DIRAC search for VOMS data in the directory pointed by ``$X509_VOMSES`` variable (if not set `/etc/vomses` will be queried).

For each VO, there should be a file with the same name of VO and filled it the following way for every VOMS server:
(Take data from http://operations-portal.egi.eu/vo)
::

   "<VO name>" "<VOMS server>" "<vomses port>" "<DN>" "<VO name>" "<https port>"

For example::

   [managai@dirac vomses]$ cat /usr/etc/vomses/superbvo.org 
   "superbvo.org" "voms2.cnaf.infn.it" "15009" "/C=IT/O=INFN/OU=Host/L=CNAF/CN=voms2.cnaf.infn.it" "superbvo.org" "8443"
   "superbvo.org" "voms-02.pd.infn.it" "15009" "/C=IT/O=INFN/OU=Host/L=Padova/CN=voms-02.pd.infn.it" "superbvo.org" "8443"

If your VO is not present, you can add the file by hand.



Operations - Shifter
--------------------
::

   Operations
   {
     SuperB-Production
     {
       Shifter
       {
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
         VOAdmin = bsanteramo
         VOMSName = pamela
         VOMSServers
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
         AutoAddVOMS = True
         AutoUploadProxy = True
         AutoUploadPilotProxy = True
       }
     }
   }

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
     }
     pamela
     {
       SuperB-Production
       {
         Shifter
         {
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

