.. _dirac_install:

.. set highlighting to console input/output
.. highlight:: console


=======================
Installing DIRAC client
=======================

The DIRAC client installation procedure consists of few steps.
You can do these steps as any user without the need to be root.

.. tabbed:: Python3

  Python3 DIRAC installations rely on the environment provided by `DIRACOS2 <https://github.com/DIRACGrid/DIRACOS2>`_.
  So, you first install DIRACOS2 and only then install DIRAC in it::

    https://github.com/DIRACGrid/DIRACOS2/releases/latest/download/DIRACOS-Linux-$(uname -m).sh
    bash DIRACOS-Linux-$(uname -m).sh
    rm DIRACOS-Linux-$(uname -m).sh
    source diracos/diracosrc

  and now DIRAC::

    pip install DIRAC==7.2

  (Just `pip install DIRAC` will install the most recent production version found on https://pypi.org/project/DIRAC/)

  And for the configuration::

    dirac-configure

.. tabbed:: Python2

  Choose the directory where you want to install the DIRAC software and run the dirac-install and dirac-configure scripts from
  this directory::

    $ wget -np -O https://raw.githubusercontent.com/DIRACGrid/management/master/dirac-install.py --no-check-certificate
    $ chmod +x dirac-install.py
    $ ./dirac-install.py -r v7r2 -t client
    $ source bashrc
    $ dirac-proxy-init --nocs
    $ dirac-configure -S DIRAC-Certification -C dips://some.whe.re:9135/Configuration/Server --SkipCAChecks

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

Updating client
===============

The client software update, when a new version is available, can be simply done by running again the ``dirac-install``
command for python2 installations, or ``pip`` for python3 installations.
