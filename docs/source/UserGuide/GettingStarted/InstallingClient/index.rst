
.. set highlighting to console input/output
.. highlight:: console

.. _dirac_install:

=======================
Installing DIRAC client
=======================

This documentation explains how to install a Dirac client. Existing installations will often pre-configure a `*rc` file for their users to source.

.. note::
  A "DIRAC client" is also a DiracX client.

The installation procedure consists of few steps.
You can do these steps as any user without the need to be root.
The procedure has been tested on recent versions of `el9` (i.e. AlmaLinux) and `debian`. We assume that it would work on most Linux distributions.

DIRAC installations rely on the environment provided by `DIRACOS2 <https://github.com/DIRACGrid/DIRACOS2>`_.
So, you first install DIRACOS2 and only then install DIRAC in it::

  $ curl -LO https://github.com/DIRACGrid/DIRACOS2/releases/latest/download/DIRACOS-Linux-$(uname -m).sh
  $ bash DIRACOS-Linux-$(uname -m).sh
  $ rm DIRACOS-Linux-$(uname -m).sh
  $ source diracos/diracosrc

and now DIRAC::

  $ pip install DIRAC

will install the most recent production versions found in https://pypi.org/project/DIRAC/. This will also install the most recent versions of DiracX.
You should now be able to tab-complete `dirac-` to see all of the DIRAC commands that are available.

At this point, you have a DIRAC/DiracX client installed, but the client needs to be connected to a server.
The configuration to connect to the server will be written down (in a "dirac.cfg" file) by the `dirac-configure` command.
Before running it, make sure that the following conditions are satisfied:

* As a user, you have a personal certificate in `.pem` format. This certificate must be stored in the `~/.globus/` directory of the machine.
* You are part of a Virtual Organization. You are registered in your VO's VOMS server (for talking to DIRAC), and in your VO's IdP server (e.g. IAM) (for talking to DiracX).
* Your VO admins have set up a DIRAC/DiracX installation to which to connect to.


For the configuration, simply issuing `dirac-configure` will start a configuration wizard, using some predefined setups. If your setup is not present, but you know the URL of DIRAC's Configuration Server, issue instead::

  $ dirac-configure -C $DIRAC_CS_URL

In both cases you will be prompted to insert the password of your personal certificate.

Using a user proxy
==================

At this point you need a proxy, so you should issue the command::

   $ dirac-proxy-init

This command will also embed a token in the proxy, in order to talk to DiracX.
You can see which file is your proxy certificate using the `dirac-proxy-info` command.

Updating client
===============

The client software update, when a new version is available, can be simply done by running again ``pip install``.
