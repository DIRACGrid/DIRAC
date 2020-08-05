.. _dirac-general-cs:

=====================================
DIRAC Section
=====================================

The *DIRAC* section contains general parameters needed in most of installation types.
In the table below options directly placed into the section are described.

  **VirtualOrganization**
    The name of the Virtual Organization of the installation User Community. The option is defined
    in a single VO installation. 
    
    ValueType: string
  
  **Setup**
    The name of the DIRAC installation Setup. This option is defined in the client installations
    to define which subset of DIRAC Systems the client will work with. See :ref:`dirac-cs-structure`
    for the description of the DIRAC configuration nomenclature.
    
    ValueType: string
    
  **Extensions**
    The list of extensions to the Core DIRAC software used by the given installation
    
    ValueType: list

*Configuration* subsection
----------------------------

The *Configuration* subsection defines several options to discover and use the configuration data
  
  *Configuration*/**Servers**
    This option defines a list of configuration servers, both master and slaves, from which clients can
    obtain the configuration data
    
    ValueType: list
    
  *Configuration*/**MasterServer**
    the URL of the Master Configuration Server. This server is used for updating the Configuration Service.
    
    ValueType: string
    
  *Configuration*/**EnableAutoMerge**
    Enables automatic merging of the modifications done in parallel by several clients
    
    ValueType: boolean
    
This subsection is used to configure the Configuration Servers attributes. It should not edited by hand since it is
upated by the Master Configuration Server to reflect the current situation of the system.

+-------------------+----------------------------------------------------+----------------------------------------------------------------------+
| **Name**          | **Description**                                    | **Example**                                                          |
+-------------------+----------------------------------------------------+----------------------------------------------------------------------+
| *AutoPublish*     |                                                    | AutoPublish = yes                                                    |
+-------------------+----------------------------------------------------+----------------------------------------------------------------------+
| *EnableAutoMerge* | Allows Auto Merge. Takes a boolean value.          | EnableAutoMerge = yes                                                |
+-------------------+----------------------------------------------------+----------------------------------------------------------------------+
| *MasterServer*    | Define the primary master server.                  | MasterServer = dips://cclcgvmli09.in2p3.fr:9135/Configuration/Server |
+-------------------+----------------------------------------------------+----------------------------------------------------------------------+
| *Name*            | Name of Configuration file                         | Name = Dirac-Prod                                                    |
+-------------------+----------------------------------------------------+----------------------------------------------------------------------+
| *PropagationTime* |                                                    | PropagationTime = 100                                                |
+-------------------+----------------------------------------------------+----------------------------------------------------------------------+
| *RefreshTime*     | How many time the secondary servers are going to   | RefreshTime = 600                                                    |
|                   | refresh configuration from master.                 |                                                                      |
|                   | Expressed as Integer and seconds as unit.          |                                                                      |
+-------------------+----------------------------------------------------+----------------------------------------------------------------------+
| *SlavesGraceTime* |                                                    | SlavesGraceTime = 100                                                |
+-------------------+----------------------------------------------------+----------------------------------------------------------------------+
| *Servers*         | List of Configuration Servers installed. Expressed | Servers = dips://cclcgvmli09.in2p3.fr:9135/Configuration/Server      |
|                   | as URLs using dips as protocol.                    |                                                                      |
+-------------------+----------------------------------------------------+----------------------------------------------------------------------+
| *Version*         | CS configuration version used by DIRAC services    | Version = 2011-02-22 15:17:41.811223                                 |
|                   | as indicator when they need to reload the          |                                                                      |
|                   | configuration. Expressed using date format.        |                                                                      |
+-------------------+----------------------------------------------------+----------------------------------------------------------------------+




*Security* subsection
------------------------

The *Security* subsection defines several options related to the DIRAC/DISET security framework

  *Security*/**UseServerCertificates**
    Flag to use server certificates and not user proxies. This is typically true for the server
    installations.
    
    ValueType: boolean
    
  *Security*/**SkipCAChecks** 
    Flag to skip the server identity by the client. The flag is usually defined in the client installations
    
    ValueType: boolean 

  *Security*/**CertFile**
    Directory where host certificate is located in the server, for example ``/opt/dirac/etc/grid-security/hostcert.pem``

  *Security*/**KeyFile**
    Directory where host key is located in the server. For example ``/opt/dirac/etc/grid-security/hostcert.pem``

.. warning:: This section should only appear in the local dirac.cfg file of each installation, never in the central configuration.

*Setups* subsection
-----------------------

The subsection defines the names of different DIRAC *Setups* as subsection names. In each subsection of the *Setup* section
the names of corresponding System instances are defined. In the example below "Production" instances of *Systems* 
Configuration and Framework are defined as part of the "Dirac-Production" *Setup*::

  DIRAC
  {
    Setups
    {
      Dirac-Production
      {
        Configuration = Production
        Framework = Production
      }
    }
  }       

For each Setup known to the installation, there must be a subsection with the appropriated name.  Each option represents
a DIRAC System available in the Setup and the Value is the instance of System that is used in that setup. For instance,
since the Configuration is unique for the whole installation, all setups should have the same instance for the
Configuration systems.
