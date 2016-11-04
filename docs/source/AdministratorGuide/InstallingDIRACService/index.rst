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

The primary server installation should install and start at least the Configuration Service which is the
backbone for the entire DIRAC system. The SystemAdministrator Service, once installed, allows remote
management of the DIRAC components on the server. In multi-server installations DIRAC components are 
distributed among a number of servers installed using the procedure for additional host installation.

For all DIRAC installations any number of client installations is possible.

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

 - As *dirac* user download the install_site.sh script::

      mkdir /home/dirac/DIRAC
      cd /home/dirac/DIRAC
      wget -np https://github.com/DIRACGrid/DIRAC/raw/integration/Core/scripts/install_site.sh --no-check-certificate

Server Certificates
-------------------

Server certificates are used for validating the identity of the host a given client is connecting to. By default 
grid host certificate include host/ in the CN (common name) field. This is not a problem for DIRAC components 
since DISET only keeps the host name after the **/** if present. 

However if the certificate is used for the Web Portal, the client validating the certificate is your browser. All browsers
will rise a security alarm if the host name in the url does not match the CN field in the certificate presented by the server.
In particular this means that *host/*, or other similar parts should nto be present, and that it is preferable to use 
DNS aliases and request a certificate under this alias in order to be able to migrate the server to a new host without
having to change your URLs. DIRAC will accept both real host names and any valid aliases without complains.

Finally, you will have to instruct you users on the procedure to upload the public key of the CA signing the certificate 
of the host where the Web Portal is running. This depends from CA to CA, but typically only means clicking on a certain 
link on the web portal of the CA.

Using your own CA
~~~~~~~~~~~~~~~~~
This is mandatory on the server running the web portal.

In case the CA certificate is not coming from traditional sources (installed using a package manager), but installed "by hand",
you need to make sure the hash of that CA certificate is created. Make sure the CA certificate is located under
``/etc/grid-security/certificates``, then do the following as root::

  cd /etc/grid-security/certificates
  openssl x509 -noout -in cert.pem -hash
  ln -s cert.pem hash.0

where the output of the ``openssl`` command gives you the hash of the certificate ``cert.pem``, and must be used for the 
``hash.0`` link name. Make sure the ``.0`` part is present in the name, as this is looked for when starting the web server.

.. _install_primary_server:

Primary server installation
---------------------------

The installation consists of setting up a set of services, agents and databases for the
required DIRAC functionality. The SystemAdministrator interface can be used later to complete 
the installation by setting up additional components. The following steps should
be taken:
 
  - Editing the installation configuration file. This file contains all
    the necessary information describing the installation. By editing the configuration 
    file one can describe the complete DIRAC server or
    just a subset for the initial setup. Below is an example of a commented configuration file.
    This file corresponds to a minimal DIRAC server configuration which allows to start
    using the system::

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
        Release = v6r10p4
        #  Python version of the installation
        PythonVersion = 26
        #  To install the Server version of DIRAC (the default is client)
        InstallType = server
        #  LCG python bindings for SEs and LFC. Specify this option only if your installation
        #  uses those services
        # LcgVer = 2012-02-20
        #  If this flag is set to yes, each DIRAC update will be installed
        #  in a separate directory, not overriding the previous ones
        UseVersionsDir = yes
        #  The directory of the DIRAC software installation
        TargetPath = /opt/dirac
        #  DIRAC extra modules to be installed (Web is required if you are installing the Portal on 
        #  this server).
        #  Only modules not defined as default to install in their projects need to be defined here: 
        #   i.e. LHCb, LHCbWeb for LHCb
        Externals = WebApp

        #
        #   These are options for the configuration of the installed DIRAC software
        #   i.e., to produce the initial dirac.cfg for the server
        #
        #  Give a Name to your User Community, it does not need to be the same name as in EGI, 
        #  it can be used to cover more than one VO in the grid sense
        VirtualOrganization = Name of your VO
        #  Site name   
        SiteName = DIRAC.HostName.ch
        #  Setup name
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
        
        #
        #  The following options define components to be installed
        #
        #  Name of the installation host (default: the current host )
        #  Used to build the URLs the services will publish
        #  For a test installation you can use 127.0.0.1
        # Host = dirac.cern.ch
        Host = 
        #  List of Services to be installed
        Services  = Configuration/Server
        Services += Framework/SystemAdministrator
        #  Flag determining whether the Web Portal will be installed
        WebPortal = yes
        #
        #  The following options defined the MySQL DB connectivity
        #
        # The following option define if you want or not install the mysql that comes with DIRAC on the machine
        # InstallMySQL = True
        Database
        {
          #  User name used to connect the DB server
          User = Dirac # default value
          #  Password for database user acess. Must be set for SystemAdministrator Service to work
          Password = XXXX
          #  Password for root DB user. Must be set for SystemAdministrator Service to work
          RootPwd = YYYY
          #  location of DB server. Must be set for SystemAdministrator Service to work
          Host = localhost # default
          #  There are 2 flags for small and large installations Set either of them to True/yes when appropriated
          # MySQLSmallMem:        Configure a MySQL with small memory requirements for testing purposes
          #                       innodb_buffer_pool_size=200MB
          # MySQLLargeMem:        Configure a MySQL with high memory requirements for production purposes
          #                       innodb_buffer_pool_size=10000MB
        }
      }

  - Run install_site.sh giving the edited configuration file as the argument. The configuration file must have
    .cfg extension (CFG file)::

      ./install_site.sh install.cfg
      
  - If the installation is successful, in the end of the script execution you will see the report
    of the status of running DIRAC services, e.g.::
          
                                  Name : Runit    Uptime    PID
                  Configuration_Server : Run          41    30268
         Framework_SystemAdministrator : Run          21    30339
                             Web_httpd : Run           5    30828
                            Web_paster : Run           5    30829
        
Now the basic services - Configuration and SystemAdministrator - are installed. The rest of the installation can proceed using 
the DIRAC Administrator interface, either command line (System Administrator Console) or using Web Portal (eventually, 
not available yet).

It is also possible to include any number of additional systems, services, agents and databases to be installed by "install_site.sh".

**Important Notice:** after executing install_site.sh (or dirac-setup-site) a runsvdir process is kept running. This 
is a watchdog process that takes care to keep DIRAC component running on your server. If you want to remove your 
installation (for instance if you are testing your install .cfg) you should first remove links from startup directory, kill the runsvdir, the runsv processes::

      #!/bin/bash
      source /opt/dirac/bashrc
      RUNSVCTRL=`which runsvctrl`
      chpst -u dirac $RUNSVCTRL d /opt/dirac/startup/*
      killall runsv svlogd
      killall runsvdir
      # If you did also installed a MySQL server uncomment the next line
      dirac-stop-mysql


.. _install_additional_server:

Additional server installation
------------------------------

To add a new server to an already existing DIRAC Installation the procedure is similar to the one above. 
You should perform all the preliminary steps to prepare the host for the installation. One additional 
operation is the registration of the new host in the already functional Configuration Service.

  - Then you edit the installation configuration file::

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
        Release = v6r3p7
        #  To install the Server version of DIRAC (the default is client)
        InstallType = server
        #  LCG python bindings for SEs and LFC. Specify this option only if your installation
        #  uses those services
        # LcgVer = 2012-02-20
        #  If this flag is set to yes, each DIRAC update will be installed
        #  in a separate directory, not overriding the previous ones
        UseVersionsDir = yes
        #  The directory of the DIRAC software installation
        TargetPath = /opt/dirac
        #  DIRAC extra packages to be installed (Web is required if you are installing the Portal on 
        #  this server).
        #  For each User Community their extra package might be necessary here: 
        #   i.e. LHCb, LHCbWeb for LHCb
        Externals = 

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
        #  List of Services to be installed
        Services  = Configuration/Server
        Services += Framework/SystemAdministrator

  - Now run install_site.sh giving the edited CFG file as the argument:::
  
        ./install_site.sh install.cfg

If the installation is successful, the SystemAdministrator service will be up and running on the
server. You can now set up the required components as described in :ref:`setting_with_CLI`

Post-Installation step
----------------------

In order to make the DIRAC components running we use the *runit* mechanism (http://smarden.org/runit/). For each component that 
must run permanently (services and agents) there is a directory created under */opt/dirac/startup* that is 
monitored by a *runsvdir* daemon. The installation procedures above will properly start this daemon. In order 
to ensure starting the DIRAC components at boot you need to add a hook in your boot sequence. A possible solution
is to add an entry in the */etc/inittab*::

      SV:123456:respawn:/opt/dirac/sbin/runsvdir-start

or if using ``upstart`` (in RHEL6 for example), add a file ``/etc/init/dirac.conf`` containing::

      start on runlevel [123456]
      stop on runlevel [0]

      respawn
      exec /opt/dirac/sbin/runsvdir-start

On specific machines, or if network is needed, it's necessary to make sure the ``runsvdir_start`` script is executed
after a certain service is started. For example, on Amazon EC2, I recommend changing the first line by::

      start on started elastic-network-interfaces


Together with a script like (it assumes that in your server DIRAC is using *dirac* local user to run)::

      #!/bin/bash
      source /opt/dirac/bashrc
      RUNSVCTRL=`which runsvctrl`
      chpst -u dirac $RUNSVCTRL d /opt/dirac/startup/*
      killall runsv svlogd
      killall runsvdir
      /opt/dirac/pro/mysql/share/mysql/mysql.server stop  --user=dirac
      sleep 10
      /opt/dirac/pro/mysql/share/mysql/mysql.server start --user=dirac
      sleep 20
      RUNSVDIR=`which runsvdir`
      exec chpst -u dirac $RUNSVDIR -P /opt/dirac/startup 'log:  DIRAC runsv'

The same script can be used to restart all DIRAC components running on the machine.

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

  - Install MySQL database. You have to enter two passwords one is the root password for MySQL itself (if not already done in the server installation) 
    and another one is the password for user who will own the DIRAC databases, in our case the user name is Dirac::

      install mysql
      MySQL root password:
      MySQL Dirac password:

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

    $ *dirac-configuration-cli*

  - In the server all the logs of the services and agents are stored and rotated in 
    files that can be checked using the following command::

      tail -f  /opt/dirac/startup/<System>_<Service or Agent>/log/current

