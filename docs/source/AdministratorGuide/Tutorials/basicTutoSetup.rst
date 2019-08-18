.. _tuto_basic_setup:

====================
Basic Tutorial setup
====================

.. set highlighting to console input/output
.. highlight:: console

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

This section is to be executed as ``root`` user.

We assume that you have at your disposition a fresh CC7 64bit installation. If you don't, we recommend installing a virtual machine. Instructions for installing CC7 can be found `here <http://linux.web.cern.ch/linux/centos7/docs/install.shtml>`_

In this tutorial, we will use a freshly installed CC7 x86_64 virtual machine, with all the default options, except the hostname being ``dirac-tuto``.

Make sure that the hostname of the machine is set to ``dirac-tuto``. Modify the ``HOSTNAME`` variable in the ``/etc/sysconfig/network`` file as such::

  HOSTNAME=dirac-tuto

Then reboot the machine and check the hostname. You should get the following output::

  [root@dirac-tuto ~]# hostname
  dirac-tuto


Machine setup
=============

This section is to be executed as ``root`` user.

Make sure that the machine can address itself using the ``dirac-tuto`` alias. Modify the ``/etc/hosts`` file as such::

  127.0.0.1   localhost localhost.localdomain localhost4 localhost4.localdomain4 dirac-tuto
  ::1         localhost localhost.localdomain localhost6 localhost6.localdomain6 dirac-tuto


-------------------------
Create the ``dirac`` user
-------------------------

The user that will run the server will be ``dirac``. Set the password for that user to ``password``,
and ensure that files below ``/opt/dirac/`` belong to this user:

.. literalinclude:: basicTutoSetup.sh
   :language: bash
   :start-after: # START add_dirac
   :end-before: # END add_dirac

-------------
Install runit
-------------

The next step is to install ``runit``, which is responsible for supervising DIRAC processes

First, install the `RPM <http://diracproject.web.cern.ch/diracproject/rpm/runit-2.1.2-1.el7.cern.x86_64.rpm>`_:

.. literalinclude:: basicTutoSetup.sh
   :language: bash
   :start-after: # START runit
   :end-before: # END runit


Create the file ``/opt/dirac/sbin/runsvdir-start``, which is responsible for starting runit, with the following content:

.. literalinclude:: basicTutoSetup.sh
   :language: bash
   :start-after: # START runsvdir-start
   :end-before: # END runsvdir-start
   :caption: /opt/dirac/sbin/runsvdir-start

Then, edit the systemd ``runsvdir-start`` service to match the following:

.. literalinclude:: basicTutoSetup.sh
   :language: bash
   :start-after: # START systemd-runsvdir
   :end-before: # END systemd-runsvdir
   :caption: /usr/lib/systemd/systemd/runsvdir-start.service

make ``runsvdir-start`` executable and (re)start ``runsvdir``:

.. literalinclude:: basicTutoSetup.sh
   :language: bash
   :start-after: # START restartrunsv
   :end-before: # END restartrunsv


-------------
Install MySQL
-------------

First of all, remove the existing (outdated) installation, and install all the necessary RPMs for MySQL 5.7:

.. literalinclude:: basicTutoSetup.sh
   :language: bash
   :start-after: # START mysqlInstall
   :end-before: # END mysqlInstall

Start the mysql service, which will then initialize itself, and, among other things, create temporary password for the
mysql ``root`` account, which needs to be changed during the first login:

.. literalinclude:: basicTutoSetup.sh
   :language: bash
   :start-after: # START mysqlStart
   :end-before: # END mysqlStart

To change the root password, create a ``mysqlSetup.sql`` file, which changes the password to a strong password, removes
a plugin to enforce the strong password (only for tutorial purposes, of course), and then sets the password to
``password``, which is easier to remember:

.. literalinclude:: basicTutoSetup.sh
   :language: mysql
   :start-after: # START mysqlSetup
   :end-before: # END mysqlSetup
   :caption: mysqlSetup.sql

Now get the temporary password from the ``/var/log/mysqld.log``, and change it using the ``mysqlSetup.sql`` file:

.. literalinclude:: basicTutoSetup.sh
   :language: bash
   :start-after: # START mysqlInit
   :end-before: # END mysqlInit

Server installation
===================

This section is to be executed as ``dirac`` user

------------------
CA and certificate
------------------

DIRAC relies on TLS for securing its connections and for authorization and authentication. Since we are using a self contained installation, we will be using our own CA. There are a bunch of utilities that we will be using to generate the necessary files.

We create a script ``setupCA`` to download utilities from the DIRAC repository and source ``utilities.sh``, and then
create the CA and certificates both for the server and the client:

.. literalinclude:: basicTutoSetup.sh
   :language: bash
   :start-after: # START setupCA
   :end-before: # END setupCA
   :caption: setupCA

Execute the script::

  bash setupCA

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

This section is to be run as ``dirac`` user in its home folder::

  sudo su dirac
  cd ~

We will install DIRAC v6r21 with DIRACOS.

First we create the ``install.cfg`` file, which is used to tell the installation script we obtain in a moment what to
install and how to configure the server with the following content:

.. literalinclude:: basicTutoSetup.sh
   :language: bash
   :start-after: # START install.cfg
   :end-before: # END install.cfg
   :caption: install.cfg

Then we download the installer, make it executable, and run it with the ``install.cfg`` file (assuming the file is in
the user's home folder):

.. literalinclude:: basicTutoSetup.sh
   :language: bash
   :start-after: # START installDirac
   :end-before: # END installDirac


The output should look something like this::

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

Create an account ``diracuser`` with password ``password``, and add in its ``~/.globus/`` directory the user
certificate you created earlier:

.. literalinclude:: basicTutoSetup.sh
   :language: bash
   :start-after: # START user_diracuser
   :end-before: # END user_diracuser


--------------------
Install DIRAC client
--------------------

This section has to be ran as ``diracuser`` in its home directory::

  sudo su diracuser
  cd

We will do the installation in the ``~/DiracInstallation`` directory. For a client, the configuration is really minimal,
so we will just install the code and its dependencies.  Create the structure, download the installer, and then install
the same version as for the server:

.. literalinclude:: basicTutoSetup.sh
   :language: bash
   :start-after: # START installClient1
   :end-before: # END installClient1

The output from the ``dirac-install.py`` command should look something like this::

  <SomeDate> dirac-install [NOTICE]  Processing installation requirements
  <SomeDate> dirac-install [NOTICE]  Destination path for installation is /home/diracuser/DIRAC
  <SomeDate> dirac-install [NOTICE]  Discovering modules to install
  <SomeDate> dirac-install [NOTICE]  Installing modules...
  <SomeDate> dirac-install [NOTICE]  Installing DIRAC:v6r21
  <SomeDate> dirac-install [NOTICE]  Retrieving http://diracproject.web.cern.ch/diracproject/tars/DIRAC-v6r21.tar.gz
  <SomeDate> dirac-install [NOTICE]  Retrieving http://diracproject.web.cern.ch/diracproject/tars/DIRAC-v6r21.md5
  <SomeDate> dirac-install [NOTICE]  Deploying scripts...
             Scripts will be deployed at /home/diracuser/DIRAC/scripts
             Inspecting DIRAC module
  <SomeDate> dirac-install [NOTICE]  Installing DIRAC OS ...
  <SomeDate> dirac-install [NOTICE]  Retrieving https://diracos.web.cern.ch/diracos/releases/diracos-1.0.0.tar.gz ...........................................................................................................................
  <SomeDate> dirac-install [NOTICE]  Retrieving https://diracos.web.cern.ch/diracos/releases/diracos-1.0.0.md5
  <SomeDate> dirac-install [NOTICE]  Fixing externals paths...
  <SomeDate> dirac-install [NOTICE]  Running externals post install...
  <SomeDate> dirac-install [NOTICE]  Creating /home/diracuser/DIRAC/bashrc
  <SomeDate> dirac-install [NOTICE]  Defaults written to defaults-DIRAC.cfg
  <SomeDate> dirac-install [NOTICE]  Executing /home/diracuser/DIRAC/scripts/dirac-externals-requirements...
  <SomeDate> dirac-install [NOTICE]  DIRAC properly installed

You will notice that among other things, the installation created a ``~/DiracInstallation/bashrc`` file. This file must be sourced whenever you want to use dirac client.

In principle, your system administrator will have managed the CA for you. In this specific case, since we have our own CA, we will just link the client installation CA with the server one:

.. literalinclude:: basicTutoSetup.sh
   :language: bash
   :start-after: # START installClient2
   :end-before: # END installClient2

The last step is to configure the client to talk to the proper configuration service. This is easily done by creating a ``~/DiracInstallation/etc/dirac.cfg`` file with the following content:

.. literalinclude:: basicTutoSetup.sh
   :language: bash
   :caption: ~/DiracInstallation/etc/dirac.cfg
   :start-after: # START dirac.cfg
   :end-before: # END dirac.cfg

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
