.. _dirac_install:


==================================
Installing DIRAC client
==================================

The DIRAC client installation procedure consists of few steps.
You can do these steps as any user, there's no need to be root.

A DIRAC client installation (and a server too) is fully in user space, and in fact, it's all in one directory.
Which means that on a same machine you can have several client (or server even) installed.

If you want to create a shared client installation, you can do it by simply giving (UNIX) access
to the directory where the client is installed.


Install script
---------------

Download the *dirac-install* script from::

  wget -np -O dirac-install https://github.com/DIRACGrid/DIRAC/raw/integration/Core/scripts/dirac-install.py --no-check-certificate
  chmod +x dirac-install

Choose the directory where you want to install the DIRAC software and run the dirac-install script from
this directory giving the appropriate version of the DIRAC release,
and, the version of the "lcgBundle" (with "-g" option) that you want to use::

  dirac-install -r v6r20p14 -g v14r2

The example above assumes that you need version v6r20p14, and that with it you are installing lcgBundle version v14r2.

An "lcgBundle" is simply a tarball containing a number of statically-compiled libraries that are used
for interacting with grid environments (e.g. GFAL2, or ARC, or Condor).
The libraries in a "lcgBundle" are not maintained within DIRAC, but DIRAC may use them.
The produced lcgBundles can be found in `this server <http://diracproject.web.cern.ch/diracproject/lcgBundles/>`_.


This installs the software and you should get the following directories and files created::

   drwxr-xr-x. 20 dirac dirac  4096 Jul 25 15:13 DIRAC
   drwxr-xr-x.  6 dirac dirac  4096 Jul 21 16:27 Linux_x86_64_glibc-2.12
   -rw-r--r--.  1 dirac dirac  2153 Jul 25 15:13 bashrc
   -rw-r--r--.  1 dirac dirac  2234 Jul 25 15:13 cshrc
   -rw-r--r--.  1 dirac dirac  42   Jul 25 15:13 defaults-DIRAC.cfg
   -rwxr-xr-x.  1 dirac dirac  61754 Jul 25 15:11 dirac-install
   drwxr-xr-x.  2 dirac dirac  12288 Jul 25 15:13 scripts


Instead of the *Linux_x86_64_glibc-2.12* directory there can be another one that corresponds to the binary platform
of your installation. The *scripts* directory contains command line tools. The *DIRAC* directory has all the
software. Finally, the *bashrc* and *cshrc* script is there to easily set up the environment for your DIRAC installation,
so assuming you are using bash::

   source bashrc

Think of adding the above line to your login scripts.

Configuring client
----------------------------

Once the client software is installed, it should be configured in order to access the corresponding DIRAC services.
The minimal necessary configuration is done by *dirac-configure* command.

The dirac-configure command can take as input a cfg file whose content can be, for example, the following::

   LocalInstallation
   {
     ConfigurationServer = dips://lbcertifdirac6.cern.ch:9135/Configuration/Server
     Setup = Dirac-Certification
   }

where the Setup option is specifying the DIRAC Setup name within which the client will be working.
The ConfigurationServer option is used to define the URL of the Configuration Service
that the client will contact to discover all the DIRAC services.

The exact values of the command options are specific for a given user community, ask the
group administrators for the details. Typically, a single community specific installation scripts are
provided which are including all the necessary specifications.

In any case, save a "install.cfg" file with the content desired.

At this point, in order to run the *dirac-configure* command, you need either a user proxy, or a host certificate.
They are needed because dirac-configure will take care of updating the local configuration,
but also because it will download the CAs used for connecting to DIRAC services
(this option may be overridden if necessary).

**Using a user proxy**

If you want to use a user proxy, we assume that you already have a user certificate,
so in this case create a directory *.globus* in your home directory and copy the certificate files
(public and pruvate keys in pem format) to this directory::

   $ mkdir ~/.globus
   $ cp <<certificate files>> ~/.globus/

At this point you need a proxy, but you still have not configured DIRAC. So, you should do::

   $ dirac-proxy-init

This will probably give you an error, but will still create a local proxy file anyway.
You can see which file is your proxy certificate using the *dirac-proxy-info* command.

It id then possible to issue the *dirac-configure* command::

   dirac-configure install.cfg

**Using a host certificate**

If you have a host certificate for the machine where the client is being installed,
and if this host certificate DN is registered in the Configuration Server of the DIRAC server
machine, then such host certificate can be used instead of the user proxy certificate,
with the following::

   dirac-configure --UseServerCertificate -o /DIRAC/Security/CertFile=<directory>/hostcert.pem -o /DIRAC/Security/KeyFile=<directory>/hostkey.pem install.cfg





Updating client
----------------

The client software update when a new version is available is simply done by running again the *dirac-install*
command giving the new version value.
