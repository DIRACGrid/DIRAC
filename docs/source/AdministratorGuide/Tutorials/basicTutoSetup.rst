.. _tuto_basic_setup:

====================
Basic Tutorial setup
====================

Tutorial goal
=============

The aim of the tutorial is to have a self contained DIRAC setup. You will be guided through the whole installation process both of the server part and the client part.
By the end of the tutorial, you will have:

* a Configuration service, to serve other servers and clients
* a ComponentMonitoring service to keep track of other services and agents installed
* a SystemAdministrator service to manage the DIRAC installation in the future
* the WebApp, to allow for web interface access

The setup you will have at the end is the base for all the other tutorials.


More links
==========

* :ref:`server_installation`

Basic requirements
==================

We assume that you have at your disposition a fresh SLC6 64bit installation. If you don't, we recommend installing a virtual machine. Instructions for installing SLC6 can be found `here <http://linux.web.cern.ch/linux/scientific6/docs/install.shtml>`_

In this tutorial, we will use a freshly installed SLC6 x86_64 virtual machine, with all the default options, except the hostname being ``dirac-tuto``.

Machine setup
=============

This section is to be executed as ``root`` user.

Make sure that the machine can address itself using the ``dirac-tuto`` alias. Modify the ``/etc/hosts`` file as such::

  127.0.0.1   localhost localhost.localdomain localhost4 localhost4.localdomain4 dirac-tuto
  ::1         localhost localhost.localdomain localhost6 localhost6.localdomain6 dirac-tuto


-------------
Install runit
-------------

The next step is to install ``runit``, which is responsible for supervising DIRAC processes

First, install the `RPM <http://diracproject.web.cern.ch/diracproject/rpm/runit-2.1.2-1.el6.x86_64.rpm>`_::

  yum install -y http://diracproject.web.cern.ch/diracproject/rpm/runit-2.1.2-1.el6.x86_64.rpm




Next, edit the ``/etc/init/runsvdir.conf`` file to point to the future DIRAC installation as such::

  # for runit - manage /usr/sbin/runsvdir-start
  start on runlevel [2345]
  stop on runlevel [^2345]
  normal exit 0 111
  respawn
  exec /opt/dirac/sbin/runsvdir-start

Finally, create the directory ``/opt/dirac/sbin``::

  mkdir -p /opt/dirac/sbin

and the file ``/opt/dirac/sbin/runsvdir-start`` with the following content::

  cd /opt/dirac
  RUNSVCTRL='/sbin/runsvctrl'
  chpst -u dirac $RUNSVCTRL d /opt/dirac/startup/*
  killall runsv svlogd
  RUNSVDIR='/sbin/runsvdir'
  exec chpst -u dirac $RUNSVDIR -P /opt/dirac/startup 'log:  DIRAC runsv'

make it executable::

  chmod +x /opt/dirac/sbin/runsvdir-start


and restart ``runsvdir``::

  restart runsvdir


-------------
Install MySQL
-------------

First of all, remove the existing (outdated) installation::

   yum remove -y $(rpm -qa | grep -i mysql | paste -sd ' ')


Install all the necessary RPMs for MySQL 5.7::

  yum install -y https://dev.mysql.com/get/Downloads/MySQL-5.7/mysql-community-devel-5.7.25-1.el6.x86_64.rpm https://dev.mysql.com/get/Downloads/MySQL-5.7/mysql-community-server-5.7.25-1.el6.x86_64.rpm https://dev.mysqlom/get/Downloads/MySQL-5.7/mysql-community-client-5.7.25-1.el6.x86_64.rpm  https://dev.mysql.com/get/Downloads/MySQL-5.7/mysql-community-libs-5.7.25-1.el6.x86_64.rpm https://dev.mysql.com/get/Downloads/MySQL-5.7/mysql-community-common-5.7.25-1.el6.x86_64.rpm


Setup the root password::

  [root@dirac-tuto ~]# mysqld_safe --skip-grant-tables &
  [1] 8840
  [root@dirac-tuto ~]# 190410 16:11:21 mysqld_safe Logging to '/var/lib/mysql/dirac-tuto.err'.
  190410 16:11:21 mysqld_safe Starting mysqld daemon with databases from /var/lib/mysql

  [root@dirac-tuto ~]# mysql -u root
  Welcome to the MySQL monitor.  Commands end with ; or \g.
  Your MySQL connection id is 1
  Server version: 5.6.43 MySQL Community Server (GPL)

  Copyright (c) 2000, 2019, Oracle and/or its affiliates. All rights reserved.

  Oracle is a registered trademark of Oracle Corporation and/or its
  affiliates. Other names may be trademarks of their respective
  owners.

  Type 'help;' or '\h' for help. Type '\c' to clear the current input statement.

  mysql> FLUSH PRIVILEGES;
  Query OK, 0 rows affected (0.00 sec)


  mysql> SET PASSWORD FOR 'root'@'localhost' = PASSWORD('password');
  Query OK, 0 rows affected (0.00 sec)

  mysql> FLUSH PRIVILEGES;
  Query OK, 0 rows affected (0.00 sec)

  mysql> quit
  Bye

  [root@dirac-tuto ~]# service mysqld stop
  Shutting down MySQL..190410 16:12:52 mysqld_safe mysqld from pid file /var/lib/mysql/dirac-tuto.pid ended
                                                            [  OK  ]
  [1]+  Done                    mysqld_safe --skip-grant-tables
  [root@dirac-tuto ~]# service mysqld start
  Starting MySQL.


-------------------------
Create the ``dirac`` user
-------------------------

The user that will run the server will be ``dirac``. You can set a password for that user::

  adduser -s /bin/bash -d /home/dirac dirac
  passwd dirac


All files below ``/opt/dirac/`` should belong to this user::

  chown -R dirac:dirac /opt/dirac/



Server installation
===================

This section is to be executed as ``dirac`` user

------------------
CA and certificate
------------------

DIRAC relies on TLS for securing its connections and for authorization and authentication. Since we are using a self contained installation, we will be using our own CA. There are a bunch of utilities that we will be using to generate the necessary files.

First of all, download the utilities from the DIRAC repository::

  mkdir ~/caUtilities/ && cd ~/caUtilities/
  curl -O -L https://raw.githubusercontent.com/DIRACGrid/DIRAC/integration/tests/Jenkins/utilities.sh
  curl -O -L https://raw.githubusercontent.com/DIRACGrid/DIRAC/integration/tests/Jenkins/config/ci/openssl_config_ca.cnf
  curl -O -L https://raw.githubusercontent.com/DIRACGrid/DIRAC/integration/tests/Jenkins/config/ci/openssl_config_host.cnf
  curl -O -L https://raw.githubusercontent.com/DIRACGrid/DIRAC/integration/tests/Jenkins/config/ci/openssl_config_user.cnf

We then will generate the CA, the host certificate, and the client certificate that will be used by our client later. First, we create a subshell, and source the tools to be able to call the functions::

  bash
  export SERVERINSTALLDIR=/opt/dirac
  export CI_CONFIG=~/caUtilities/
  source utilities.sh


Then we generate the CA::

  [dirac@dirac-tuto caUtilities]$ generateCA
  ==> [generateCA]
  Generating RSA private key, 2048 bit long modulus
  .............+++
  ...............+++
  e is 65537 (0x10001)

Now generate a host certificate, valid for 1 year::

  [dirac@dirac-tuto ca]$ generateCertificates 365
  ==> [generateCertificates]
  Using configuration from /opt/dirac/etc/grid-security/ca/openssl_config_ca.cnf
  Check that the request matches the signature
  Signature ok
  Certificate Details:
          Serial Number: 4096 (0x1000)
          Validity
              Not Before: Apr 10 14:47:38 2019 GMT
              Not After : Apr  9 14:47:38 2020 GMT
          Subject:
              countryName               = ch
              organizationName          = DIRAC
              organizationalUnitName    = DIRAC CI
              commonName                = dirac-tuto
              emailAddress              = lhcb-dirac-ci@cern.ch
          X509v3 extensions:
              X509v3 Basic Constraints:
                  CA:FALSE
              Netscape Comment:
                  OpenSSL Generated Server Certificate
              X509v3 Subject Key Identifier:
                  85:90:F4:7D:6E:31:50:F7:3E:53:7E:0B:B3:22:D5:5C:37:D4:D0:5A
              X509v3 Authority Key Identifier:
                  keyid:33:F0:C8:60:6D:6B:52:BD:E9:A7:FA:57:27:72:5A:5D:7E:43:12:ED
                  DirName:/O=DIRAC CI/CN=DIRAC CI Signing Certification Authority
                  serial:88:B1:7A:54:17:8C:00:13

              X509v3 Key Usage: critical
                  Digital Signature, Key Encipherment
              X509v3 Extended Key Usage:
                  TLS Web Server Authentication, TLS Web Client Authentication
              X509v3 Subject Alternative Name:
                  DNS:dirac-tuto, DNS:localhost
  Certificate is to be certified until Apr  9 14:47:38 2020 GMT (365 days)

  Write out database with 1 new entries
  Data Base Updated


Finally, generate the client certificate for later, also valid one year::

  [dirac@dirac-tuto grid-security]$ generateUserCredentials 365
  ==> [generateUserCredentials]
  Generating RSA private key, 2048 bit long modulus
  ................................................................................+++
  ...........................................................................................................................................+++
  e is 65537 (0x10001)
  Using configuration from /opt/dirac/etc/grid-security/ca/openssl_config_ca.cnf
  Check that the request matches the signature
  Signature ok
  Certificate Details:
          Serial Number: 4097 (0x1001)
          Validity
              Not Before: Apr 10 14:48:31 2019 GMT
              Not After : Apr  9 14:48:31 2020 GMT
          Subject:
              countryName               = ch
              organizationName          = DIRAC
              organizationalUnitName    = DIRAC CI
              commonName                = ciuser
              emailAddress              = lhcb-dirac-ci@cern.ch
          X509v3 extensions:
              X509v3 Basic Constraints:
                  CA:FALSE
              X509v3 Subject Key Identifier:
                  98:BB:F0:A8:96:4F:80:C8:3E:21:60:5E:FD:17:4E:34:97:EF:31:17
              X509v3 Authority Key Identifier:
                  keyid:33:F0:C8:60:6D:6B:52:BD:E9:A7:FA:57:27:72:5A:5D:7E:43:12:ED

              X509v3 Key Usage: critical
                  Digital Signature, Non Repudiation, Key Encipherment
              X509v3 Extended Key Usage:
                  TLS Web Client Authentication
              Netscape Comment:
                  OpenSSL Generated Client Certificate
  Certificate is to be certified until Apr  9 14:48:31 2020 GMT (365 days)

  Write out database with 1 new entries
  Data Base Updated

To finish, time to exit the subshell::

  exit


At this point, you should find:

* The CA in ``/opt/dirac/etc/grid-security/certificates``::

    [dirac@dirac-tuto caUtilities]$ ls /opt/dirac/etc/grid-security/certificates/
    855f710d.0  ca.cert.pem

* The host certificate (``hostcert.pem``) and key (``hostkey.pem``) in ``/opt/dirac/etc/grid-security``::

    [dirac@dirac-tuto caUtilities]$ ls /opt/dirac/etc/grid-security/
    ca  certificates  hostcert.pem  hostkey.pem  openssl_config_host.cnf  request.csr.pem

* The user credentials for later in ``/opt/dirac/user/``::

    [dirac@dirac-tuto caUtilities]$ ls /opt/dirac/user/
    client.key  client.pem  client.req  openssl_config_user.cnf

--------------------
Install DIRAC Server
--------------------

This section is to be run as ``dirac`` user.

We will install DIRAC v6r21 with DIRACOS.

First, download the installer, and make it executable::

  mkdir ~/DiracInstallation && cd ~/DiracInstallation
  curl -O -L https://github.com/DIRACGrid/DIRAC/raw/integration/Core/scripts/install_site.sh
  chmod +x install_site.sh


``install_site.sh`` requires a configuration file to tell it what and how to install. Create a file called ``install.cfg`` with the following content::

  LocalInstallation
  {
    #  DIRAC release version to install
    Release = v6r21p3
    #  Installation type
    InstallType = server
    #  Each DIRAC update will be installed in a separate directory, not overriding the previous ones
    UseVersionsDir = yes
    #  The directory of the DIRAC software installation
    TargetPath = /opt/dirac
    #  Install the WebApp extension
    Extensions = WebApp

    # Name of the VO we will use
    VirtualOrganization = tutoVO
    # Name of the site or host
    SiteName = dirac-tuto
    # Setup name
    Setup = MyDIRAC-Production
    #  Default name of system instances
    InstanceName = Production
    #  Flag to skip download of CAs
    SkipCADownload = yes
    #  Flag to use the server certificates
    UseServerCertificate = yes

    # Name of the Admin user (from the user certificate we created )
    AdminUserName = ciuser
    # DN of the Admin user certificate (from the user certificate we created)
    AdminUserDN = /C=ch/O=DIRAC/OU=DIRAC CI/CN=ciuser/emailAddress=lhcb-dirac-ci@cern.ch
    AdminUserEmail= adminUser@cern.ch
    # Name of the Admin group
    AdminGroupName = dirac_admin

    # DN of the host certificate (from the host certificate we created)
    HostDN = /C=ch/O=DIRAC/OU=DIRAC CI/CN=dirac-tuto/emailAddress=lhcb-dirac-ci@cern.ch
    # Define the Configuration Server as Master
    ConfigurationMaster = yes

    # List of DataBases to be installed (what's here is a list for a basic installation)
    Databases = InstalledComponentsDB
    Databases += ResourceStatusDB

    #  List of Services to be installed (what's here is a list for a basic installation)
    Services  = Configuration/Server
    Services += Framework/ComponentMonitoring
    Services += Framework/SystemAdministrator
    #  Flag determining whether the Web Portal will be installed
    WebPortal = yes
    WebApp = yes

    Database
    {
      #  User name used to connect the DB server
      User = Dirac
      #  Password for database user access
      Password = Dirac
      #  Password for root DB user
      RootPwd = password
      #  location of DB server
      Host = localhost
    }
  }


And then run it::


  [dirac@dirac-tuto DIRAC]$ ./install_site.sh --dirac-os install.cfg
  --2019-04-11 08:51:21--  https://github.com/DIRACGrid/DIRAC/raw/integration/Core/scripts/dirac-install.py
  Resolving github.com... 140.82.118.4, 140.82.118.3
  Connecting to github.com|140.82.118.4|:443... connected.
  HTTP request sent, awaiting response... 302 Found


  [...]


  Status of installed components:

    Name                          Runit Uptime PID
  =================================================
  1 Web_WebApp                    Run        4 24338
  2 Configuration_Server          Run       53 24142
  3 Framework_ComponentMonitoring Run       36 24207
  4 Framework_SystemAdministrator Run       20 24247


You can verify that the components are running::

  [dirac@dirac-tuto DIRAC]$ runsvstat /opt/dirac/startup/*
  /opt/dirac/startup/Configuration_Server: run (pid 24142) 288 seconds
  /opt/dirac/startup/Framework_ComponentMonitoring: run (pid 24207) 271 seconds
  /opt/dirac/startup/Framework_SystemAdministrator: run (pid 24247) 255 seconds
  /opt/dirac/startup/Web_WebApp: run (pid 24338) 239 seconds


The logs are to be found in ``/opt/dirac/runit/``, grouped by component.

The installation created the file ``/opt/dirac/etc/dirac.cfg``. The content is the same as the ``install.cfg``, with the addition of the following::

  DIRAC
  {
    Setup = MyDIRAC-Production
    VirtualOrganization = tutoVO
    Extensions = WebApp
    Security
    {
    }
    Setups
    {
      MyDIRAC-Production
      {
        Configuration = Production
        Framework = Production
      }
    }
    Configuration
    {
      Master = yes
      Name = MyDIRAC-Production
      Servers = dips://dirac-tuto:9135/Configuration/Server
    }
  }
  LocalSite
  {
    Site = dirac-tuto
  }
  Systems
  {
    Databases
    {
      User = Dirac
      Password = Dirac
      Host = localhost
      Port = 3306
    }
    NoSQLDatabases
    {
      Host = dirac-tuto
      Port = 9200
    }
  }

This part is used as configuration for all your services and agents that you will run. It contains two important information:

* The database credentials
* The address of the configuration server: ``Servers = dips://dirac-tuto:9135/Configuration/Server``

The Configuration service will serve the content of the file ``/opt/dirac/etc/MyDIRAC-Production.cfg`` to every client, be it a service, an agent, a job, or an interactive client. The content looks like such::

  DIRAC
  {
    Extensions = WebApp
    VirtualOrganization = tutoVO
    Configuration
    {
      Name = MyDIRAC-Production
      Version = 2019-04-11 06:52:18.414086
      MasterServer = dips://dirac-tuto:9135/Configuration/Server
    }
    Setups
    {
      MyDIRAC-Production
      {
        Configuration = Production
        Framework = Production
      }
    }
  }
  Registry
  {
    Users
    {
      ciuser
      {
        DN = /C=ch/O=DIRAC/OU=DIRAC CI/CN=ciuser/emailAddress=lhcb-dirac-ci@cern.ch
        Email = adminUser@cern.ch
      }
    }
    Groups
    {
      dirac_user
      {
        Users = ciuser
        Properties = NormalUser
      }
      dirac_admin
      {
        Users = ciuser
        Properties = AlarmsManagement
        Properties += ServiceAdministrator
        Properties += CSAdministrator
        Properties += JobAdministrator
        Properties += FullDelegation
        Properties += ProxyManagement
        Properties += Operator
      }
    }
    Hosts
    {
      dirac-tuto
      {
        DN = /C=ch/O=DIRAC/OU=DIRAC CI/CN=dirac-tuto/emailAddress=lhcb-dirac-ci@cern.ch
        Properties = TrustedHost
        Properties += CSAdministrator
        Properties += JobAdministrator
        Properties += FullDelegation
        Properties += ProxyManagement
        Properties += Operator
      }
    }
    DefaultGroup = dirac_user
  }
  Operations
  {
    Defaults
    {
      EMail
      {
        Production = adminUser@cern.ch
        Logging = adminUser@cern.ch
      }
    }
  }
  WebApp
  {
    Access
    {
      upload = TrustedHost
    }
  }
  Systems
  {
    Framework
    {
      Production
      {
        Services
        {
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
          SystemAdministrator
          {
            Port = 9162
            Authorization
            {
              Default = ServiceAdministrator
              storeHostInfo = Operator
            }
          }
        }
        URLs
        {
          ComponentMonitoring = dips://dirac-tuto:9190/Framework/ComponentMonitoring
          SystemAdministrator = dips://dirac-tuto:9162/Framework/SystemAdministrator
        }
        FailoverURLs
        {
        }
        Databases
        {
          InstalledComponentsDB
          {
            DBName = InstalledComponentsDB
            Host = localhost
            Port = 3306
          }
        }
      }
    }
  }


This configuration will be used for example by Services in order to:

* know their configuration (for example the ``ComponentMonitoring`` Service will use everything under ``Systems/Framework/Production/Services/ComponentMonitoring`` )
* Identify host and persons (``Registry`` section)

Or by clients to get the URLs of given services (for example ``ComponentMonitoring = dips://dirac-tuto:9190/Framework/ComponentMonitoring``)

Since this configuration is given as a whole to every client, you understand why no database credentials are in this file. Services and Agents running on the machine will have their configuration as a merge of what is served by the Configuration service and the ``/opt/dirac/etc/dirac.cfg``, and thus have access to these private information.

The file ``/opt/dirac/bashrc`` is to be sourced whenever you want to use the server installation.

Client installation
===================

Now we will create another linux account ``diracuser`` and another installation to be used as client

--------------------
Setup client session
--------------------

This section has to be ran as ``root``

First, create an account, and add in its ``~/.globus/`` directory the user certificate you created earlier::

  adduser -s /bin/bash -d /home/diracuser diracuser
  passwd diracuser
  mkdir ~diracuser/.globus/
  cp /opt/dirac/user/client.pem ~diracuser/.globus/usercert.pem
  cp /opt/dirac/user/client.key ~diracuser/.globus/userkey.pem
  chown -R diracuser:diracuser ~diracuser/.globus/


--------------------
Install DIRAC client
--------------------

This section has to be ran as ``diracuser``

We will do the installation in the ``~/DiracInstallation`` directory. For a client, the configuration is really minimal, so we will just install the code and its dependencies.
First, create the structure, and download the installer::

  mkdir ~/DiracInstallation && cd ~/DiracInstallation
  curl -O -L https://github.com/DIRACGrid/DIRAC/raw/integration/Core/scripts/dirac-install.py
  chmod +x dirac-install.py


Now we trigger the installation, with the same version as the server::

  [diracuser@dirac-tuto DIRAC]$ ./dirac-install.py -r v6r21 --dirac-os
  2019-04-11 14:46:41 UTC dirac-install [NOTICE]  Processing installation requirements
  2019-04-11 14:46:41 UTC dirac-install [NOTICE]  Destination path for installation is /home/diracuser/DIRAC
  2019-04-11 14:46:41 UTC dirac-install [NOTICE]  Discovering modules to install
  2019-04-11 14:46:41 UTC dirac-install [NOTICE]  Installing modules...
  2019-04-11 14:46:41 UTC dirac-install [NOTICE]  Installing DIRAC:v6r21
  2019-04-11 14:46:41 UTC dirac-install [NOTICE]  Retrieving http://diracproject.web.cern.ch/diracproject/tars/DIRAC-v6r21.tar.gz
  2019-04-11 14:46:41 UTC dirac-install [NOTICE]  Retrieving http://diracproject.web.cern.ch/diracproject/tars/DIRAC-v6r21.md5
  2019-04-11 14:46:42 UTC dirac-install [NOTICE]  Deploying scripts...
  Scripts will be deployed at /home/diracuser/DIRAC/scripts
  Inspecting DIRAC module
  2019-04-11 14:46:42 UTC dirac-install [NOTICE]  Installing DIRAC OS ...
  2019-04-11 14:46:42 UTC dirac-install [NOTICE]  Retrieving https://diracos.web.cern.ch/diracos/releases/diracos-1.0.0.tar.gz
  .........................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................2019-04-11 14:46:46 UTC dirac-install [NOTICE]  Retrieving https://diracos.web.cern.ch/diracos/releases/diracos-1.0.0.md5
  2019-04-11 14:47:02 UTC dirac-install [NOTICE]  Fixing externals paths...
  2019-04-11 14:47:02 UTC dirac-install [NOTICE]  Running externals post install...
  2019-04-11 14:47:02 UTC dirac-install [NOTICE]  Creating /home/diracuser/DIRAC/bashrc
  2019-04-11 14:47:02 UTC dirac-install [NOTICE]  Defaults written to defaults-DIRAC.cfg
  2019-04-11 14:47:02 UTC dirac-install [NOTICE]  Executing /home/diracuser/DIRAC/scripts/dirac-externals-requirements...
  2019-04-11 14:47:03 UTC dirac-install [NOTICE]  DIRAC properly installed

You will notice that among other things, the installation created a ``~/DiracInstallation/bashrc`` file. This file must be sourced whenever you want to use dirac client.

In principle, your system administrator will have managed the CA for you. In this specific case, since we have our own CA, we will just link the client installation CA with the server one::

  mkdir -p ~/DiracInstallation/etc/grid-security/
  ln -s /opt/dirac/etc/grid-security/certificates/ ~/DiracInstallation/etc/grid-security/certificates

The last step is to configure the client to talk to the proper configuration service. This is easily done by creating a ``~/DiracInstallation/etc/dirac.cfg`` file with the following content::

  DIRAC
  {
    Setup = MyDIRAC-Production
    Configuration
    {
      Servers = dips://dirac-tuto:9135/Configuration/Server
    }
  }

You should now be able to get a proxy::

  [diracuser@dirac-tuto DIRAC]$ source ~/DiracInstallation/bashrc
  [diracuser@dirac-tuto DIRAC]$ dirac-proxy-init
  Generating proxy...
  Proxy generated:
  subject      : /C=ch/O=DIRAC/OU=DIRAC CI/CN=ciuser/emailAddress=lhcb-dirac-ci@cern.ch/CN=460648814
  issuer       : /C=ch/O=DIRAC/OU=DIRAC CI/CN=ciuser/emailAddress=lhcb-dirac-ci@cern.ch
  identity     : /C=ch/O=DIRAC/OU=DIRAC CI/CN=ciuser/emailAddress=lhcb-dirac-ci@cern.ch
  timeleft     : 23:59:59
  DIRAC group  : dirac_user
  rfc          : True
  path         : /tmp/x509up_u501
  username     : ciuser
  properties   : NormalUser


And you can observe that the Configuration Service has served the client::

  [diracuser@dirac-tuto DIRAC]$ grep ciuser /opt/dirac/runit/Configuration/Server/log/current
  2019-04-11 14:54:10 UTC Configuration/Server NOTICE: Executing action ([::1]:33394)[dirac_user:ciuser] RPC/getCompressedDataIfNewer(<masked>)
  2019-04-11 14:54:10 UTC Configuration/Server NOTICE: Returning response ([::1]:33394)[dirac_user:ciuser] (0.00 secs) OK

--------------
Use the WebApp
--------------

This section is to be executed as ``diracuser``.

First you need to convert your user certificate into a ``p12`` format (you will be prompt for a password, you can leave it empty)::

  cd ~/.globus/
  openssl pkcs12 -export -out certificate.p12 -inkey userkey.pem -in usercert.pem

This will create the file ``~/.globus/certificate.p12``.

Use your favorite browser, and add this certificate.

You should be able to access the WebApp using the following address ``https://localhost:8443/DIRAC/``


Conclusion
==========

We have seen how to install a DIRAC server and client using a personal CA, and how to access the WebApp. Starting from here, you will be able to extend on further tutorials.
