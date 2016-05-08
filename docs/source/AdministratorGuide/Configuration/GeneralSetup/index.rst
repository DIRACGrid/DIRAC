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

*Setups* subsection
-----------------------

The subsection defines the names of different DIRAC *Setups* as subsection names. In each subsection of the *Setup* section
the names of corresponding System instances are defined. In the example below "Production" instances of *Systems* 
Configuration and Framework are defined as part of the "Dirac-Prduction" *Setup*::

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
    
       
