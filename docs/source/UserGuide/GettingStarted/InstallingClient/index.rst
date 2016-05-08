==================================
Installing DIRAC client 
==================================

The DIRAC client installation procedure consists of several steps.

.. _dirac_install:

Install script
---------------

Download the *dirac-install* script from::

  wget -np -O dirac-install https://github.com/DIRACGrid/DIRAC/raw/integration/Core/scripts/dirac-install.py --no-check-certificate
  chmod +x dirac-install
  
Choose the directory where you want to install the DIRAC software and run the dirac-install script from
this directory giving the appropriate version of the DIRAC release::

  dirac-install -r v5r9

This installs the software and you should get the following directories and files created::

    drwxr-xr-x  8 atsareg lhcb  2048 Mar 25  2010 Linux_x86_64_glibc-2.5
    drwxr-xr-x 16 atsareg lhcb  2048 Oct 12 12:13 DIRAC
    -rwxr-xr-x  1 atsareg lhcb 21224 Oct 12 13:37 dirac-install
    drwxr-xr-x  2 atsareg lhcb 10240 Oct 12 17:11 scripts
    -rw-r--r--  1 atsareg lhcb   998 Oct 12 17:15 bashrc  
    
Instead of the *Linux_x86_64_glibc-2.5* directory there can be another one that corresponds to the binary platform
of your installation. The *scripts* directory contains command line tools. The *DIRAC* directory has all the 
software. Finally, the *bashrc* script is there to easily set up the environment for your DIRAC installation::

   source bashrc
   
Think of adding the above line to your login scripts.

Installing with VO defaults
----------------------------

In most cases you are installing the DIRAC client to work as a member of some particular user community or, in 
other words, Virtual Organization. The managers of your Virtual Organization can prepare default settings to
be applied for the DIRAC client installation. In this case the installation procedure reduces to the following
assuming the name of the Virtual Organization *dirac*::

  wget -np -O dirac-install http://lhcbproject.web.cern.ch/lhcbproject/dist/Dirac_project/dirac-install --no-check-certificate
  chmod +x dirac-install
  dirac-install -V formation
  source bashrc
  dirac-proxy-init
  dirac-configure defaults_formation.cfg
   
The *dirac_defaults.cfg* file contains the Virtual Organization default settings. It is downloaded as part of
the installation procedure. Check with your Virtual Organization managers if this mode of installation is 
available.  
   
Configuring client
----------------------------   
    
Once the client software is installed, it should be configured in order to access the corresponding DIRAC services. 
The minimal necessary configuration is done by the following command::

   dirac-configure -V dirac -S Dirac-Production -C dips://dirac.in2p3.fr:9135/Configuration/Server 
   
where -S option is specifying the DIRAC Setup name within which the client will be working. The -C option
is to define the URL of the Configuration Service that the client will contact to discover all the DIRAC
services. The exact values of the command options are specific for a given user community, ask the
group administrators for the details. Typically, a single community specific installation scripts are
provided which are including all the necessary specifications.

Updating client
----------------

The client software update when a new version is available is simply done by running again the *dirac-install*
command giving the new version value.
