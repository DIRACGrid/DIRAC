=============================================
Step 1: Minimal Framework Installation
=============================================

Before doing any DIRAC server installation you should have a look at :ref:`server_installation`, in particular
the sections :ref:`server_requirements` and :ref:`server_preparation`. After you have created the necessary
directory structure and placed the host certificate in the proper location, you are ready for this first Step.

In this Step, the procedure for any server installation is shown. It consists of three different phases:

 - Installation of the DIRAC code.

 - Creation of the initial DIRAC local configuration file.

 - Deployment of the necessary DIRAC components

The first 2 phases are common to all Steps. The code installation phase can be skipped since all components will
use the same code. In some cases additional local configuration will be necessary, and thus the second phase will
need to be repeated. While the third phase will always be necessary to add new functionality to the installation.


A Minimal DIRAC installation
------------------------------------

The minimal set of components that required for a DIRAC server are a *Configuration Server* and the *System Administrator*
services. Additionally one can add the *Security Logging* and the *Bundle Delivery services*. The first one receives a summary
of all connections received by all DIRAC services in the current installation. The second allows any DIRAC client to download
an up-to-date version of CA's public keys and Certification Revocation List, CRL.

The way to achieve this minimal installation is the following:

 - Download the *dirac-install* as described in :ref:`dirac_install`.

 - Create a *Step_1.cfg* file using the following template and substituting strings within *[ ]* by appropriate values for your case::

    #
    # This section determines which DIRAC components will be installed and where
    #
    LocalInstallation
    {
      #
      #   These are options for the installation of the DIRAC software
      #
      #  DIRAC release version (this is an example, you should find out the current
      #  production release)
      Release = [The version to be installed. Default: HEAD]
      #  Python version os the installation
      PythonVersion = 27
      #  To install the Server version of DIRAC (the default is client)
      InstallType = server
      #  LCG python bindings for SEs and LFC. Specify this option only if your installation
      #  uses those services
      # LcgVer = v14r2
      #  If this flag is set to yes, each DIRAC update will be installed
      #  in a separate directory, not overriding the previous ones
      UseVersionsDir = yes
      #  The directory of the DIRAC software installation
      TargetPath = /opt/dirac
      #  DIRAC extensions to be installed (Web is required if you are installing the Portal on
      #  this server).
      #  For each User Community their own extension might be necessary here:
      #   i.e. LHCb, LHCbWeb for LHCb
      Extensions = Web

      #
      #   These are options for the configuration of the installed DIRAC software
      #   i.e., to produce the initial dirac.cfg for the server
      #
      #  Give a Name to your User Community, it does not need to be the same name as in EGI,
      #  it can be used to cover more than one VO in the grid sense
      VirtualOrganization = MyVO
      #  Site name: it should follow the convention [Infrastructure].[name].[country code]
      SiteName = [The name for your installation site. I.e. DIRAC.ubuntu.es]
      #  Setup name
      Setup = MyDIRAC-Production
      #  Default name of system instances
      InstanceName = Production
      #  Flag to use the server certificates
      UseServerCertificate = yes
      #  Do not download CAs, CRLs
      SkipCADownload = yes
      #  Configuration Server URL (This should point to the URL of at least one valid Configuration
      #  Service in your installation, for the primary server it should not used)
      ConfigurationServer = dips://localhost:9135/Configuration/Server
      #  Flag to set up the Configuration Server as Master (use only in the primary server)
      ConfigurationMaster = yes
      #  Configuration Name
      ConfigurationName = MyConfiguration

      #
      #   These options define the DIRAC components to be installed on "this" DIRAC server.
      #
      #
      #  The next options should only be set for the primary server,
      #  they properly initialize the configuration data
      #
      #  Name of the Admin user (default: None )
      AdminUserName = [Your short name for the DIRAC installation. I.e. ricardo]
      #  DN of the Admin user certificate (default: None )
      #  In order the find out the DN that needs to be included in the Configuration for a given
      #  host or user certificate the following command can be used:
      #
      #          openssl x509 -noout -subject -enddate -in <certfile.pem>
      #
      AdminUserDN = [The DN of your grid certificate. I.e. /DC=es/DC=irisgrid/O=ecm-ub/CN=Ricardo-Graciani-Diaz]
      #  Email of the Admin user (default: None )
      AdminUserEmail = [Your email. I.e. graciani@ecm.ub.es]
      #  Name of the Admin group (default: dirac_admin )
      # AdminGroupName = dirac_admin
      #  Name of the installation host (default: the current host )
      #  Used to build the URLs the services will publish
      #  This will only allow to make local tests on this installation
      Host =localhost
      #  DN of the host certificate (default: None )
      #  In order the find out the DN that needs to be included in the Configuration for a given
      #  host or user certificate the following command can be used:
      #
      #          openssl x509 -noout -subject -enddate -in <certfile.pem>
      #
      HostDN = [The DN of the host grid certificate. I.e. /DC=ch/DC=cern/OU=computers/CN=volhcb19.cern.ch]

      #
      #  Components to deploy
      #
      Systems = Configuration, Framework
      Services = Configuration/Server
      Services += Framework/SecurityLogging
      Services += Framework/BundleDelivery
      Services += Framework/SystemAdministrator

    }

 - Execute the installation of the DIRAC code::

   > ./dirac-install Step_1.cfg

 - Produce the initial configuration file::

   > source bashrc
   > dirac-configure Step_1.cfg

 - Deploy the requested components::

   > dirac-setup-site
