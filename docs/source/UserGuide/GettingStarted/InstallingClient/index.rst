.. _dirac_install:

.. set highlighting to console input/output
.. highlight:: console


=======================
Installing DIRAC client
=======================

The DIRAC client installation procedure consists of few steps.
You can do these steps as any user without the need to be root.


Python3 client installations
----------------------------

Choose the directory where you want to install the DIRAC client and::

   $ curl -LO https://github.com/DIRACGrid/DIRACOS2/releases/latest/download/DIRACOS-Linux-$(uname -m).sh
   $ bash DIRACOS-Linux-$(uname -m).sh
   $ rm DIRACOS-Linux-$(uname -m).sh
   $ source diracos/diracosrc
   $ pip install DIRAC (for the latest production version)
   $ dirac-configure (and follow instructions)


Python2 client installations
----------------------------

Choose the directory where you want to install the DIRAC software and run the dirac-install and dirac-configure scripts from
this directory::

   $ wget -np -O https://raw.githubusercontent.com/DIRACGrid/management/master/dirac-install.py --no-check-certificate
   $ chmod +x dirac-install.py
   $ ./dirac-install.py -r v7r2 -t client
   $ source bashrc
   $ dirac-proxy-init --nocs
   $ dirac-configure -S DIRAC-Certification -C dips://some.whe.re:9135/Configuration/Server --SkipCAChecks

The example above assumes that you need the release version v7r2.
 


Using a user proxy
==================

If you want to use a user proxy, we assume that you already have a user certificate,
so in this case create a directory *.globus* in your home directory and copy the certificate files
(public and private keys in .pem (Privacy Enhanced Mail format) to this directory::

   $ mkdir ~/.globus
   $ cp <<certificate files>> ~/.globus/

At this point you need a proxy, but you still have not configured DIRAC. So, you should issue the command::

   $ dirac-proxy-init

You can see which file is your proxy certificate using the *dirac-proxy-info* command.
