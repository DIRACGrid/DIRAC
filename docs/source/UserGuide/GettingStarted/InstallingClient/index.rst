.. _dirac_install:


=======================
Installing DIRAC client
=======================

The DIRAC client installation procedure consists of few steps.
You can do these steps as any user without the need to be root.

A DIRAC client installation (and a server too) is fully in user space, and in fact, it's all in one directory,
which means that on the same machine you can have several client(s) (or even server(s)) installed.

If you want to create a shared client installation, you can do it by simply giving (UNIX) access
to the directory where the client is installed.


Install script (for python2 clients)
------------------------------------

Choose the directory where you want to install the DIRAC software and run the dirac-install and dirac-configure scripts from
this directory::

   wget -np https://raw.githubusercontent.com/DIRACGrid/management/master/dirac-install.py --no-check-certificate
   chmod +x dirac-install.py
   ./dirac-install.py -r |version| -t client
   source bashrc
   dirac-proxy-init --nocs --no-upload
   dirac-configure -S DIRAC-Certification -C dips://some.whe.re:9135/Configuration/Server --SkipCAChecks

The example above assumes that you need the release version |version|.
 
**Using a user proxy**

If you want to use a user proxy, we assume that you already have a user certificate,
so in this case create a directory *.globus* in your home directory and copy the certificate files
(public and private keys in .pem (Privacy Enhanced Mail format) to this directory::

   $ mkdir ~/.globus
   $ cp <<certificate files>> ~/.globus/

At this point you need a proxy, but you still have not configured DIRAC. So, you should issue the command::

   $ dirac-proxy-init

This will probably give you an error, but will still create a local proxy file anyway.
You can see which file is your proxy certificate using the *dirac-proxy-info* command.

Updating client
----------------

The client software update, when a new version is available, can be simply done by running again the *dirac-install*
command but this time giving the new version value.
