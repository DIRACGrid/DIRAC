.._data-management-system

======================
Data Management System
======================

.. toctree::
   :maxdepth: 1
   
   agents
   fts

.. contents:: Table of contents
   :depth: 4
  

The DIRAC Data Management System (DMS), together with the DIRAC Storage Management System (SMS) provides the necessary functionality to execute and control all activities related with your data. the DMS provides from the basic functionality to upload a local file in a StorageElement (SE) and register the corresponding replica in the FileCatalog (FC) to massive data replications using FTS or retrievals of data archived on Tape for it later processing.

To achieve this functionality the DMS and SMS require a proper description of the involved external servers (SE, FTS, etc.) as well as a number of Agents and associated Servers that animate them. In the following sections the different aspects of each functional component are explained in some detail.

---------------
StorageElements
---------------

DIRAC provides an abstraction of a SE interface that allows to access different kind of them with a single interface. The access to each kind of SE ( SRMv2, DIRAC SE, ...) is achieved by using specific plugin modules that provide a common interface. The information necessary to define the proper plugin module and to properly configure this plugin to access a certain SE has to be introduced in the DIRAC :ref:`Configuration <dirac-cs-structure>`. An example of such configuration is::

    CERN-USER
    {
      ReadAccess = Active
      WriteAccess = Active
      AccessProtocol.1
      {
        PluginName = SRM2
        Access = remote
        Protocol = srm
        Host = srm-lhcb.cern.ch
        Port = 8443
        WSUrl = /srm/managerv2?SFN=
        Path = /castor/cern.ch/grid
        SpaceToken = LHCb_USER
      }
    }

In order to factorize the configuration, it is possible to use BaseSE, which acts just like inheritance in object programming. You define a SE just like any other. This SE can then be refered to by another SE. This new SE will inherit all the configuration from its parents, and can override it.  For example::

   CERN-EOS
   {
     BackendType = Eos
     SEType = T0D1
     AccessProtocol.1
     {
       Host = srm-eoslhcb.cern.ch
       Port = 8443
       PluginName = GFAL2_SRM2
       Protocol = srm
       Path = /eos/lhcb/grid/prod
       Access = remote
       SpaceToken = LHCb-EOS
       WSUrl = /srm/v2/server?SFN=
     }
   }
   CERN-DST-EOS
   {
     BaseSE = CERN-EOS
   }
   CERN-USER
   {
     BaseSE = CERN-EOS
     PledgedSpace = 205
     AccessProtocol.1
     {
      PluginName = GFAL2_SRM2
      Path = /eos/lhcb/grid/user
      SpaceToken = LHCb_USER
     }
   }
   
   
This definition would be strictly equivalent to::

   CERN-EOS
   {
     BackendType = Eos
     SEType = T0D1
     AccessProtocol.1
     {
       Host = srm-eoslhcb.cern.ch
       Port = 8443
       PluginName = GFAL2_SRM2
       Protocol = srm
       Path = /eos/lhcb/grid/prod
       Access = remote
       SpaceToken = LHCb-EOS
       WSUrl = /srm/v2/server?SFN=
     }
   }
   CERN-DST-EOS
   {
      BackendType = Eos
      SEType = T0D1
      AccessProtocol.1
      {
        Host = srm-eoslhcb.cern.ch
        Port = 8443
        PluginName = GFAL2_SRM2
        Protocol = srm
        Path = /eos/lhcb/grid/prod
        Access = remote
        SpaceToken = LHCb-EOS
        WSUrl = /srm/v2/server?SFN=
      }
   }
   CERN-USER
   {
      BackendType = Eos
      SEType = T0D1
      PledgedSpace = 205
      AccessProtocol.1
      {
        Host = srm-eoslhcb.cern.ch
        Port = 8443
        PluginName = GFAL2_SRM2
        Protocol = srm
        Path = /eos/lhcb/grid/user
        Access = remote
        SpaceToken = LHCb_USER
        WSUrl = /srm/v2/server?SFN=
      }
   }

Note that in case of overwriting, the protocol sections are matched using the PluginName attribute, and not the section name, which is irrelevant at all.

--------------
Multi Protocol
--------------

There are several aspects of multi protocol:
   - One SE supports several protocols
   - SEs with different protocols need to interact
   - We want to use different protocols for different operations
   
DIRAC supports all of them. The bottom line is that before executing an action on an SE, we check among all the plugins defined for it, which plugins are the most suitable.
There are 4 Operation options under the `DataManagement` section used for that:

  - `RegistrationProtocols`: used to generate a URL that will be stored in the FileCatalog
  - `AccessProtocols`: used to perform the read operations
  - `WriteProtocols`: used to perform the write and remove operations
  - `ThirdPartyProtocols`: used in case of replications
  
When performing an action on an SE, the StorageElement class will evaluate, based on these lists, and following this preference order, which StoragePlugins to use.
The behavior is straightforward for simple read or write actions. It is however a bit more tricky when it comes to third party copies.

Each StoragePlugins has a list of protocols that it is able to accept as input and a list that it is able to generate. In most of the cases, for protocol X, the plugin
is able to generate URL for the protocol X, and to take as input URL for the protocol X and local files. There are plugins that can do more, like GFAL2_SRM2 plugins
that can handle many more (xroot, gsiftp, etc). It may happen that the SE can be writable only by one of the protocol. Suppose the following situation: you want to replicate
from storage A to storage B. Both of them have as plugins GFAL2_XROOT and GFAL2_SRM2; AccessProtocols is "root,srm", WriteProtocols is "srm" and ThirdPartyProtocols is "root,srm".
The negociation between the storages to find common protocol for third party copy will lead to "root,srm". Since we follow the order, the sourceURL will be a root url,
and it will be generated by GFAL2_XROOT because root is its native protocol (so we avoid asking the srm server for a root turl). The destination will only consider using
GFAL2_SRM2 plugins because only srm is allowed as a write plugin, but since this plugins can take root URL as input, the copy will work.


The WriteProtocols and AccessProtocols list can be locally overwritten in the SE definition.
  

----------------------
FTS transfers in DIRAC
----------------------

DIRAC DMS can be configured to make use of FTS servers in order to schedule and monitor efficient transfer of large amounts of data between SEs. As of today, FTS servers are only able to handle transfers between SRM SEs. You will need to define at least two different SRM StorageElements in your Configuration and one FTS endpoint. In the current implementation of the DIRAC FTS interface FTS transfers are always assigned to the FTS server associated to the Site local to the destination SE. However you can associate the same FTS server to more than one site.

In order to configure and test support for FTS transfers in your DIRAC installation you should follow these steps:

- Make sure that there are FTS servers configured for the use of your VirtualOrganization. You can find this out, for instance, by using the "lcg-infosites"  command of a gLite User Interface:

::

 $ lcg-infosite --vo <name of your VO> fts
 $ # for instance:
 $ lcg-infosites --vo lhcb fts
 https://fts.pic.es:8443/glite-data-transfer-fts/services/FileTransfer
 https://fts-fzk.gridka.de:8443/glite-data-transfer-fts/services/FileTransfer
 https://fts.cr.cnaf.infn.it:8443/glite-data-transfer-fts/services/FileTransfer
 https://fts.grid.sara.nl:8443/glite-data-transfer-fts/services/FileTransfer
 https://cclcgftsprod.in2p3.fr:8443/glite-data-transfer-fts/services/FileTransfer
 https://lcgfts.gridpp.rl.ac.uk:8443/glite-data-transfer-fts/services/FileTransfer
 https://fts-t2-service.cern.ch:8443/glite-data-transfer-fts/services/FileTransfer
 https://fts22-t0-export.cern.ch:8443/glite-data-transfer-fts/services/FileTransfer


- Determine which channels are supported on a particular FTS server. You can know that using the command gLite command "glite-transfer-channel-list". You need to use the -s option and pass one of the above URLs replacing "FileTransfer" by "ChannelManagement". Channels are list with the format Site1-Site2, STAR is a keyword that applies to any site.

::

 $ glite-transfer-channel-list -s https://fts.pic.es:8443/glite-data-transfer-fts/services/ChannelManagement
 STAR-NCG
 STAR-LIPCOIMBRA
 BSC-PIC
 LAPALMA-PIC
 PIC-NCG
 STAR-PIC
 ...
 $ glite-transfer-channel-list -s https://fts.pic.es:8443/glite-data-transfer-fts/services/ChannelManagement STAR-PIC
 Channel: STAR-PIC
 Between: * and PIC
 State: Active
 Contact: fts-support@pic.es
 Bandwidth: 0
 Nominal throughput: 0
 Number of files: 50, streams: 5
 Number of VO shares: 5
 VO 'atlas' share is: 50
 VO 'cms' share is: 50
 VO 'dteam' share is: 50
 VO 'lhcb' share is: 50
 VO 'ops' share is: 50
 
   
- Include the URL of the FTS server in the DIRAC Configuration:

::

 # This is an example, use the name and URL corresponding to your case
 /Resources/FTSEndpoints/LCG.PIC.es =  https://fts.pic.es:8443/glite-data-transfer-fts/services/FileTransfer

- Now you need to make sure that the DIRAC components that take care of FTS transfers are in place. You need to configure and startup a number of components. This can be done with the "dirac-setup-server" command and a the following FTS.cfg describing what you need:

::

      LocalInstallation
      {
        Systems = DataManagement, RequestManagement
        DataBases = RequestDB
        Services = DataManagement/TransferDBMonitoring
        Agents = DataManagement/FTSSubmitAgent, DataManagement/FTSMonitorAgent
      }

- Then one needs to configure the DIRAC Channels that will be handled by the FTS Agents. The methods to create and manipulate the DIRAC Channels for FTS are not exposed on a Service interface. This has to be done with a simple python script from the server:

::

   from DIRAC.Core.Base import Script
   Script.parseCommandLine()
   from DIRAC.DataManagementSystem.DB.TransferDB import TransferDB
   
   sourceSite = 'ShortSite-Name1'         # LCG.CERN.ch -> CERN
   destinationSite = 'ShortSite-Name2'

   transferDB = TransferDB()

   res = transferDB.createChannel( sourceSite, destinationSite )
   if not res['OK']:
     print res['Message']
     exit(-1)

   channelID = res['Value']
   print 'Created FTS Channel %s' % channelID

- At this point some transfer can be attempted between the configured SEs. For that purpose you can use the command line script:

::

 $ dirac-dms-fts-submit -h
   Submit an FTS request, monitor the execution until it completes
 Usage:
   dirac-dms-fts-submit [option|cfgfile] ... LFN sourceSE targetSE
 Arguments:
   LFN:      Logical File Name or file containing LFNs
   sourceSE: Valid DIRAC SE
   targetSE: Valid DIRAC SE
 General options:
   -o:  --option=         : Option=value to add
   -s:  --section=        : Set base section for relative parsed options
   -c:  --cert=           : Use server certificate to connect to Core Services
   -d   --debug           : Set debug mode (-dd is extra debug)
   -h   --help            : Shows this help

::

  $ dirac-dms-fts-submit /lhcb/user/r/rgracian/fts_test CNAF-USER PIC-USER
  Submitted b3c7c25a-1d14-11e1-abe9-dc229ac9908c @ https://fts.pic.es:8443/glite-data-transfer-fts/services/FileTransfer
  |====================================================================================================>| 100.0% Finished


Using this script, the request to the FTS server will be formulated following the information configured in DIRAC, and will be submitted form your client to the selected FTS server with your local credential. Make sure you are using a proxy that is authorized at your FTS server (usually only some specific users in the VO are allowed, contact the administrators of the site offering you this server in case of doubts).

.. include:: agents.rst
.. include:: data_logging_system.rst
