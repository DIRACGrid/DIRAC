.. _server_installation:

=========================
DIRAC Server Installation
=========================

The procedure described here outlines the installation of the DIRAC components on a host machine, a
DIRAC server. There are two distinct cases of installations:

- *Primary server installation*. This the first installation of a fresh new DIRAC system. No functioning
  Configuration Service is running yet (:ref:`install_primary_server`).
- *Additional server installation*. This is the installation of additional hosts connected to an already
  existing DIRAC system, with the Master Configuration Service already up and running on another
  DIRAC server (:ref:`install_additional_server`).

The primary server installation should install and start at least the following services,
which constitute what is considered as a minimal DIRAC installation:

- The *Configuration Service (CS)*: the CS is backbone for the entire DIRAC system.
  Please refer to :ref:`dirac-configuration` for more information
- The *SystemAdministrator* service which, once installed, allows remote
  management of the DIRAC components directly on the server.
- The *Component Monitoring* service is for keeping track of installed components.
  Refer to :ref:`static_component_monitoring` for more info.
- The *Resource Status* service will keep track of the status of your distributed computing resources.
  Refer to :ref:`resource_status_system` for more info.

In multi-server installations DIRAC components are
distributed among a number of servers installed using the procedure for additional host installation.

For all DIRAC installations any number of client installations is possible.


Using Puppet
------------

The procedure outlined below is a manual procedure for installing DIRAC.
Some installations have been done using `puppet <https://puppet.com/>`_.
Find puppet modules used at CERN in https://gitlab.cern.ch/ai/it-puppet-module-dirac.


.. _server_requirements:


Requirements
------------

*Server:*

- 9130-9200 ports should be open in the firewall for the incoming TCP/IP connections (this is the
  default range if predefined ports are used, the port on which services are listening can be
  configured by the DIRAC administrator)::

   iptables -I INPUT -p tcp --dport 9130:9200 -j ACCEPT
   service iptables save

- DIRAC extensions that need specific services which are not an extension of DIRAC used
  should better use ports 9201-9300 in order to avoid confusion. If this happens,
  the procedure above should be repeated to include the new range of ports.
- For the server hosting the portal, ports 80 and 443 should be open and redirected to ports
  8080 and 8443 respectively, i.e. setting iptables appropriately::

   iptables -t nat -I PREROUTING -p tcp --dport 80 -j REDIRECT --to-ports 8080
   iptables -t nat -I PREROUTING -p tcp --dport 443 -j REDIRECT --to-ports 8443

  If you have problems with NAT or iptables you can use multipurpose relay *socat*::

   socat TCP4-LISTEN:80,fork TCP4:localhost:8080 &
   socat TCP4-LISTEN:443,fork TCP4:localhost:8443 &

- Grid host certificates in pem format;
- At least one of the servers of the installation must have updated CAs and CRLs files; if you want to install
   the standard Grid CAs you can follow the instructions at https://wiki.egi.eu/wiki/EGI_IGTF_Release. They
   are usally installed /etc/grid-security/certificates. You may also need to install the ``fetch-crl`` package,
   and run the ``fetch-crl`` command once installed.
- If gLite third party services are needed (for example, for the pilot job submission via WMS
  or for data transfer using FTS) gLite User Interface must be installed and the environment set up
  by "sourcing" the corresponding script, e.g. /etc/profile.d/grid-env.sh.

*Client:*

- User certificate and private key in .pem format in the $HOME/.globus directory with correct
  permissions.
- User certificate loaded into the Web Browser (currently supported browsers are: Mozilla Firefox, Chrome
  and Safari)

.. _server_preparation:

Server preparation
------------------

Any host running DIRAC server components should be prepared before the installation of DIRAC following
the steps below. This procedure must be followed for the primary server and for any additional server installations.

- As *root* create a *dirac* user account. This account will be used to run all the DIRAC components::

     adduser -s /bin/bash -d /home/dirac dirac

- As *root*, create the directory where the DIRAC services will be installed::

     mkdir /opt/dirac
     chown -R dirac:dirac /opt/dirac

- As *root*, check that the system clock is exact. Some system components are generating user certificate proxies
  dynamically and their validity can be broken because of the wrong system date and time. Properly configure
  the NTP daemon if necessary.

- As *dirac* user, create directories for security data and copy host certificate::

     mkdir -p /opt/dirac/etc/grid-security/
     cp hostcert.pem hostkey.pem /opt/dirac/etc/grid-security

  In case your host certificate is in the p12 format, you can convert it with::

     openssl pkcs12 -in host.p12 -clcerts -nokeys -out hostcert.pem
     openssl pkcs12 -in host.p12 -nocerts -nodes -out hostkey.pem

  Make sure the permissions are set right correctly, such that the hostkey.pem is only readable by the ``dirac`` user.
- As *dirac* user, create a directory or a link pointing to the CA certificates directory, for example::

     ln -s /etc/grid-security/certificates  /opt/dirac/etc/grid-security/certificates

  (this is only mandatory in one of the servers. Others can be synchronized from this one using DIRAC tools.)

- As *dirac* user download the install_site.sh script. (note the download location varies depending on the Python version you wish to use!)::

     mkdir /home/dirac/DIRAC
     cd /home/dirac/DIRAC
     # For Python 2 based installations
     curl -O https://raw.githubusercontent.com/DIRACGrid/DIRAC/integration/src/DIRAC/Core/scripts/install_site.sh
     # For Python 3 based installations
     curl -O https://raw.githubusercontent.com/DIRACGrid/management/master/install_site.sh


----------------
Installing runit
----------------

In order to make the DIRAC components running we use the *runit* mechanism (http://smarden.org/runit/).

As *dirac* user, create ``/opt/dirac/sbin directory`` and create the file ``/opt/dirac/sbin/runsvdir-start`` with the following content, and make it executable::

  #!/bin/bash
  cd /opt/dirac
  RUNSVCTRL='/sbin/runsvctrl'
  chpst -u dirac $RUNSVCTRL d /opt/dirac/startup/*
  killall runsv svlogd
  RUNSVDIR='/sbin/runsvdir'
  exec chpst -u dirac $RUNSVDIR -P /opt/dirac/startup 'log:  DIRAC runsv'

This section must be executed as *root*

Install the `RPM <http://diracproject.web.cern.ch/diracproject/rpm/runit-2.1.2-1.el7.cern.x86_64.rpm>`__.

Edit the file ``/usr/lib/systemd/system/runsvdir-start.service`` to the following::

  [Unit]
  Description=Runit Process Supervisor

  [Service]
  ExecStart=/opt/dirac/sbin/runsvdir-start
  Restart=always
  KillMode=process

  [Install]
  WantedBy=multi-user.target

Reload the configuration and restart::

  systemctl daemon-reload
  systemctl restart runsvdir-start
  systemctl enable runsvdir-start

Server Certificates
-------------------

Server certificates are used for validating the identity of the host a given client is connecting to. We follow the RFC 6125.
Basically, that means that the DNS name used to contact the host must be present in the ``SubjectAlternativeName``.

Couple notes:

* SAN in your certificates: if you are contacting a machine using its aliases, make sure that all the aliases are in the SubjectAlternativeName (SAN) field of the certificates
* FQDN in the configuration: SAN normally contains only FQDN, so make sure you use the FQDN in the CS as well (e.g. ``mymachine.cern.ch`` and not ``mymachine``)

.. _using_own_CA:

-----------------
Using your own CA
-----------------

This is mandatory on the server running the web portal.

In case the CA certificate is not coming from traditional sources (installed using a package manager), but installed "by hand",
you need to make sure the hash of that CA certificate is created. Make sure the CA certificate is located under
``/etc/grid-security/certificates``, then do the following as root::

  cd /etc/grid-security/certificates
  openssl x509 -noout -in cert.pem -hash
  ln -s cert.pem hash.0

where the output of the ``openssl`` command gives you the hash of the certificate ``cert.pem``, and must be used for the
``hash.0`` link name. Make sure the ``.0`` part is present in the name, as this is looked for when starting the web server.


MySQL database preparation
--------------------------

Before proceeding with the primary server installation, a MYSQL server must be available.
DIRAC supports MySQL versions 5.7, 8.0.
In addition to the root/admin user(s) the following users must be created, with the same PASSWORD::

   CREATE USER 'Dirac'@'%' IDENTIFIED BY '[PASSWORD]';
   CREATE USER 'Dirac'@'localhost' IDENTIFIED BY '[PASSWORD]';
   CREATE USER 'Dirac'@'[DB-SERVER-HOSTNAME]' IDENTIFIED BY '[PASSWORD]';


.. _install_primary_server:

Primary server installation
---------------------------

The installation consists of setting up a set of services, agents and databases for the
required DIRAC functionality. The SystemAdministrator interface can be used later to complete
the installation by setting up additional components. The following steps should
be taken based on the Python version you wish to install.

.. tabbed:: For Python 3

  - Edit the installation configuration file. This file contains all
    the necessary information describing the installation. By editing the configuration
    file one can describe the complete DIRAC server or
    just a subset for the initial setup. Below is an example of a commented configuration file.
    This file corresponds to a minimal DIRAC server configuration which allows to start
    using the system:

    .. dropdown:: Minimal DIRAC server configuration which allows to start using the system
      :animate: fade-in

      ::

        #
        # This section determines which DIRAC components will be installed and where
        #
        LocalInstallation
        {
          #
          #   These are options for the configuration of the installed DIRAC software
          #   i.e., to produce the initial dirac.cfg for the server
          #
          #  Give a Name to your User Community, it does not need to be the same name as in EGI,
          #  it can be used to cover more than one VO in the grid sense
          VirtualOrganization = Name of your VO
          #  Site name
          SiteName = DIRAC.HostName.ch
          #  Setup name (every installation can have multiple setups, but give a name to the first one)
          Setup = MyDIRAC-Production
          #  Default name of system instances
          InstanceName = Production
          #  Flag to skip download of CAs, on the first Server of your installation you need to get CAs
          #  installed by some external means
          SkipCADownload = yes
          #  Flag to use the server certificates
          UseServerCertificate = yes
          #  Configuration Server URL (This should point to the URL of at least one valid Configuration
          #  Service in your installation, for the primary server it should not used )
          #  ConfigurationServer = dips://myprimaryserver.name:9135/Configuration/Server
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
          AdminUserName = adminusername
          #  DN of the Admin user certificate (default: None )
          #  In order the find out the DN that needs to be included in the Configuration for a given
          #  host or user certificate the following command can be used::
          #
          #          openssl x509 -noout -subject -enddate -in <certfile.pem>
          #
          AdminUserDN = /DC=ch/aminDN
          #  Email of the Admin user (default: None )
          AdminUserEmail = adminmail@provider
          #  Name of the Admin group (default: dirac_admin )
          AdminGroupName = dirac_admin
          #  DN of the host certificate (*) (default: None )
          HostDN = /DC=ch/DC=country/OU=computers/CN=computer.dn
          # Define the Configuration Server as Master for your installations
          ConfigurationMaster = yes
          # List of Systems to be installed - by default all services are added
          Systems = Accounting
          Systems += Configuration
          Systems += DataManagement
          Systems += Framework
          Systems += Monitoring
          Systems += Production
          Systems += RequestManagement
          Systems += ResourceStatus
          Systems += StorageManagement
          Systems += Transformation
          Systems += WorkloadManagement
          #
          # List of DataBases to be installed (what's here is a list for a basic installation)
          Databases = InstalledComponentsDB
          Databases += ResourceStatusDB
          #
          #  The following options define components to be installed
          #
          #  Name of the installation host (default: the current host )
          #  Used to build the URLs the services will publish
          #  For a test installation you can use 127.0.0.1
          # Host = dirac.cern.ch
          #  List of Services to be installed (what's here is a list for a basic installation)
          Services  = Configuration/Server
          Services += Framework/ComponentMonitoring
          Services += Framework/SystemAdministrator
          Services += ResourceStatus/ResourceStatus
          #  Flag determining whether the Web Portal will be installed
          WebPortal = yes
          #
          #  The following options defined the MySQL DB connectivity
          Database
          {
            #  User name used to connect the DB server
            User = Dirac # default value
            #  Password for database user acess. Must be set for SystemAdministrator Service to work
            Password = XXXX
            #  Password for root DB user. Must be set for SystemAdministrator Service to work
            RootPwd = YYYY
            #  location of DB server. Must be set for SystemAdministrator Service to work
            Host = localhost # default, otherwise a FQDN
            Port = 3306 # default, otherwise the port
          }
        }

    or You can download the full server installation from::

      curl https://github.com/DIRACGrid/DIRAC/raw/integration/src/DIRAC/Core/scripts/install_full_py3.cfg -o install.cfg

  - Run install_site.sh giving the edited configuration file as the argument. The configuration file must have
    .cfg extension (CFG file). While not strictly necessary, it's advised that a version is added with the '-v' switch
    (pick the most recent one, see release notes in https://raw.githubusercontent.com/DIRACGrid/DIRAC/integration/release.notes)::

      ./install_site.sh install.cfg

.. tabbed:: For Python 2

  - Edit the installation configuration file. This file contains all
    the necessary information describing the installation. By editing the configuration
    file one can describe the complete DIRAC server or
    just a subset for the initial setup. Below is an example of a commented configuration file.
    This file corresponds to a minimal DIRAC server configuration which allows to start
    using the system:

    .. dropdown:: Minimal DIRAC server configuration which allows to start using the system
      :animate: fade-in

      ::

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
          Release = v7r2p8
          #  To install the Server version of DIRAC (the default is client)
          InstallType = server
          #  If this flag is set to yes, each DIRAC update will be installed
          #  in a separate directory, not overriding the previous ones
          UseVersionsDir = yes
          #  The directory of the DIRAC software installation
          TargetPath = /opt/dirac
          #  DIRAC extra modules to be installed (Web is required if you are installing the Portal on
          #  this server).
          #  Only modules not defined as default to install in their projects need to be defined here:
          #   i.e. LHCb, LHCbWeb for LHCb
          Extensions = WebApp

          #
          #   These are options for the configuration of the installed DIRAC software
          #   i.e., to produce the initial dirac.cfg for the server
          #
          #  Give a Name to your User Community, it does not need to be the same name as in EGI,
          #  it can be used to cover more than one VO in the grid sense
          VirtualOrganization = Name of your VO
          #  Site name
          SiteName = DIRAC.HostName.ch
          #  Setup name (every installation can have multiple setups, but give a name to the first one)
          Setup = MyDIRAC-Production
          #  Default name of system instances
          InstanceName = Production
          #  Flag to skip download of CAs, on the first Server of your installation you need to get CAs
          #  installed by some external means
          SkipCADownload = yes
          #  Flag to use the server certificates
          UseServerCertificate = yes
          #  Configuration Server URL (This should point to the URL of at least one valid Configuration
          #  Service in your installation, for the primary server it should not used )
          #  ConfigurationServer = dips://myprimaryserver.name:9135/Configuration/Server
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
          AdminUserName = adminusername
          #  DN of the Admin user certificate (default: None )
          #  In order the find out the DN that needs to be included in the Configuration for a given
          #  host or user certificate the following command can be used::
          #
          #          openssl x509 -noout -subject -enddate -in <certfile.pem>
          #
          AdminUserDN = /DC=ch/aminDN
          #  Email of the Admin user (default: None )
          AdminUserEmail = adminmail@provider
          #  Name of the Admin group (default: dirac_admin )
          AdminGroupName = dirac_admin
          #  DN of the host certificate (*) (default: None )
          HostDN = /DC=ch/DC=country/OU=computers/CN=computer.dn
          # Define the Configuration Server as Master for your installations
          ConfigurationMaster = yes
          # List of Systems to be installed - by default all services are added
          Systems = Accounting
          Systems += Configuration
          Systems += DataManagement
          Systems += Framework
          Systems += Monitoring
          Systems += Production
          Systems += RequestManagement
          Systems += ResourceStatus
          Systems += StorageManagement
          Systems += Transformation
          Systems += WorkloadManagement
          #
          # List of DataBases to be installed (what's here is a list for a basic installation)
          Databases = InstalledComponentsDB
          Databases += ResourceStatusDB
          #
          #  The following options define components to be installed
          #
          #  Name of the installation host (default: the current host )
          #  Used to build the URLs the services will publish
          #  For a test installation you can use 127.0.0.1
          # Host = dirac.cern.ch
          #  List of Services to be installed (what's here is a list for a basic installation)
          Services  = Configuration/Server
          Services += Framework/ComponentMonitoring
          Services += Framework/SystemAdministrator
          Services += ResourceStatus/ResourceStatus
          #  Flag determining whether the Web Portal will be installed
          WebPortal = yes
          WebApp = yes
          #
          #  The following options defined the MySQL DB connectivity
          Database
          {
            #  User name used to connect the DB server
            User = Dirac # default value
            #  Password for database user acess. Must be set for SystemAdministrator Service to work
            Password = XXXX
            #  Password for root DB user. Must be set for SystemAdministrator Service to work
            RootPwd = YYYY
            #  location of DB server. Must be set for SystemAdministrator Service to work
            Host = localhost # default, otherwise a FQDN
            Port = 3306 # default, otherwise the port
          }
        }

    or You can download the full server installation from::

      curl https://raw.githubusercontent.com/DIRACGrid/DIRAC/integration/src/DIRAC/Core/scripts/install_full_py2.cfg -o install.cfg

  - Run install_site.sh giving the edited configuration file as the argument. The configuration file must have
    .cfg extension (CFG file). While not strictly necessary, it's advised that a version is added with the '-v' switch
    (pick the most recent one, see release notes in https://raw.githubusercontent.com/DIRACGrid/DIRAC/integration/release.notes)::

      ./install_site.sh -v v7r2p8 install.cfg

Primary server installation (continued)
---------------------------------------

- If the installation is successful, in the end of the script execution you will see the report
  of the status of running DIRAC services, e.g.::

                                Name : Runit    Uptime    PID
                Configuration_Server : Run          41    30268
       Framework_SystemAdministrator : Run          21    30339
       Framework_ComponentMonitoring : Run          11    30340
       ResourceStatus_ResourceStatus : Run           9    30341

Now the basic services - Configuration, SystemAdministrator, ComponentMonitoring and ResourceStatus - are installed,
or at least their DBs should be installed, and their services up and running.

There are anyway a couple more steps that should be done to fully activate the ComponentMonitoring and the ResourceStatus.
These steps can be found in the respective administration sessions of this documentation:

- :ref:`static_component_monitoring` for the static component monitoring (the ComponentMonitoring service)
- :ref:`rss_installation` and :ref:`rss_populate` for the Resource Status System

but, no hurry: you can do it later.

The rest of the installation can proceed using the DIRAC Administrator interface,
either command line (System Administrator Console) or using Web Portal (eventually, not available yet).

It is also possible to include any number of additional systems, services, agents and databases to be installed by "install_site.sh".

.. note::
   After executing install_site.sh (or dirac-setup-site) a runsvdir process is kept running. This
   is a watchdog process that takes care to keep DIRAC component running on your server. If you want to remove your
   installation (for instance if you are testing your install .cfg) you should first remove links from startup directory, kill the runsvdir, the runsv processes::

      #!/bin/bash
      source /opt/dirac/bashrc
      RUNSVCTRL=`which runsvctrl`
      chpst -u dirac $RUNSVCTRL d /opt/dirac/startup/*
      killall runsv svlogd
      killall runsvdir

.. _install_additional_server:

Additional server installation
------------------------------

To add a new server to an already existing DIRAC Installation the procedure is similar to the one above.
You should perform all the preliminary steps to prepare the host for the installation. One additional
operation is the registration of the new host in the already functional Configuration Service.


.. tabbed:: For Python 3

  - Then you edit the installation configuration file:

    .. dropdown:: Additional DIRAC server configuration
      :animate: fade-in

      ::

        #
        # This section determines which DIRAC components will be installed and where
        #
        LocalInstallation
        {
          #
          #   These are options for the configuration of the previously installed DIRAC software
          #   i.e., to produce the initial dirac.cfg for the server
          #
          #  Give a Name to your User Community, it does not need to be the same name as in EGI,
          #  it can be used to cover more than one VO in the grid sense
          VirtualOrganization = Name of your VO
          #  Site name
          SiteName = DIRAC.HostName2.ch
          #  Setup name
          Setup = MyDIRAC-Production
          #  Default name of system instances
          InstanceName = Production
          #  Flag to use the server certificates
          UseServerCertificate = yes
          #  Configuration Server URL (This should point to the URL of at least one valid Configuration
          #  Service in your installation, for the primary server it should not used)
          ConfigurationServer = dips://myprimaryserver.name:9135/Configuration/Server
          ConfigurationServer += dips://localhost:9135/Configuration/Server
          #  Configuration Name
          ConfigurationName = MyConfiguration

          #
          #   These options define the DIRAC components being installed on "this" DIRAC server.
          #   The simplest option is to install a slave of the Configuration Server and a
          #   SystemAdministrator for remote management.
          #
          #  The following options defined components to be installed
          #
          #  Name of the installation host (default: the current host )
          #  Used to build the URLs the services will publish
          # Host = dirac.cern.ch
          Host =
          #  List of Services to be installed --- every host MUST have a Framework/SystemAdministrator service installed
          Services = Framework/SystemAdministrator
          # Service +=
        }

  - Now run install_site.sh giving the edited CFG file as the argument:::

        ./install_site.sh install.cfg

  If the installation is successful, the SystemAdministrator service will be up and running on the
  server. You can now set up the required components as described in :ref:`setting_with_CLI`

.. tabbed:: For Python 2

  - Then you edit the installation configuration file:

    .. dropdown:: Additional DIRAC server configuration
      :animate: fade-in

      ::

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
          Release = v7r2p8
          #  To install the Server version of DIRAC (the default is client)
          InstallType = server
          #  If this flag is set to yes, each DIRAC update will be installed
          #  in a separate directory, not overriding the previous ones
          UseVersionsDir = yes
          #  The directory of the DIRAC software installation
          TargetPath = /opt/dirac
          #  DIRAC extra packages to be installed (Web is required if you are installing the Portal on
          #  this server).
          #  For each User Community their extra package might be necessary here:
          #   i.e. LHCb, LHCbWeb for LHCb
          # Externals =

          #  The following options defined components to be installed
          #
          #  Name of the installation host (default: the current host )
          #  Used to build the URLs the services will publish
          # Host = dirac.cern.ch
          Host =
          #  List of Services to be installed --- every host MUST have a Framework/SystemAdministrator service installed
          Services = Framework/SystemAdministrator
          # Service +=
        }

  - Now run install_site.sh giving the edited CFG file as the argument:::

        ./install_site.sh -v v7r2p8 install.cfg

  If the installation is successful, the SystemAdministrator service will be up and running on the
  server. You can now set up the required components as described in :ref:`setting_with_CLI`

.. _setting_with_CLI:

Setting up DIRAC services and agents using the System Administrator Console
---------------------------------------------------------------------------

To use the :ref:`system-admin-console`, you will need first to install the DIRAC Client software on some machine.
To install the DIRAC Client, follow the procedure described in the User Guide.

- Start admin command line interface using administrator DIRAC group::

    dirac-proxy-init -g dirac_admin
    dirac-admin-sysadmin-cli --host <HOST_NAME>

    where the HOST_NAME is the name of the DIRAC service host

- At any time you can use the help command to get further details::

    dirac.pic.es >help

    Documented commands (type help <topic>):
    ========================================
    add   execfile  install  restart  show   stop
    exec  exit      quit     set      start  update

    Undocumented commands:
    ======================
    help

- Add instances of DIRAC systems which service or agents will be running on the server, for example::

    add instance WorkloadManagement Production

- Install databases, for example::

    install db ComponentMonitoringDB

- Install services and agents, for example::

    install service WorkloadManagement JobMonitoring
    ...
    install agent Configuration CE2CSAgent

Note that all the necessary commands above can be collected in a text file and the whole installation can be
accomplished with a single command::

      execfile <command_file>

Component Configuration and Monitoring
----------------------------------------

At this point all the services should be running with their default configuration parameters.
To change the components configuration parameters

- Login into web portal and choose dirac_admin group, you can change configuration file following these links::

    Systems -> Configuration -> Manage Configuration

- Use the comand line interface to the Configuration Service::

  $ dirac-configuration-cli

- In the server all the logs of the services and agents are stored and rotated in
  files that can be checked using the following command::

    tail -f  /opt/dirac/startup/<System>_<Service or Agent>/log/current
