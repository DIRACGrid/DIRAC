.. _https_services:

===============================
Note on HTTPs services in DIRAC
===============================

.. contents::

Background
**********

For many years, DIRAC services have been exposed through the historical DISET (``dips://``) protocol.
Few years ago DIRAC developers started exposing DIRAC services using the HTTPs protocol.
This page explains how to migrate existing DIPs protocol to HTTPs. For a developer view, refer to :ref:`httpsTornado`.

For a summary presentation you can check `this <https://indico.cern.ch/event/852597/contributions/4331720/attachments/2241040/3799728/HttpsInDIRAC.pdf>`_.

NB: not all DIPs services can be exposed through HTTPs. For a comprehensve list, please refer to :ref:`scalingLimitations`.

General principle
=================

Contrary to the DISET protocol where each service would have its own process and port opened, HTTPs services are served by a unique process and port, based on Tornado.

There are exceptions to that, due to the use of global variables in some parts of DIRAC. Namely:

* Master CS

All other services can follow the standard procedure described below.

First, the following configuration subsections have to be added to CS::

  # "Main" section
  DIRAC
  {
    Setups
    {
      ...
      Tornado = Production  # "Production" is common here, for your setup what follow what's already there for the other systems
    }
  }

  # Add Tornado to Systems section
  Systems
  {
    ...
    Tornado
    {
      Production
      {
        Port = 443
      }
    }
  }


Installation of an HTTPs based service
======================================

Just run ``dirac-install-component`` with the service you are interested in, for example
``dirac-install-component WorkloadManagement/JobMonitoring``. This will install an ``runit`` component running ``tornado-start-all``.

Alternatively, use ``dirac-admin-sysadmi-cli``.

Case: you are already running the equivalent DISET service
-----------------------------------------------------------

You can (before or after installing the tornado-based service) fully remove the DISET version of the service with ``dirac-uninstall-component DataManagement JobMonitoring``

Example of configuration before/after:

.. literalinclude:: /../../src/DIRAC/WorkloadManagementSystem/ConfigTemplate.cfg
   :start-after: ##BEGIN JobMonitoring
   :end-before: ##END
   :dedent: 2
   :caption: JobMonitoring configuration for DISET

.. literalinclude:: /../../src/DIRAC/WorkloadManagementSystem/ConfigTemplate.cfg
   :start-after: ##BEGIN TornadoJobMonitoring
   :end-before: ##END
   :dedent: 2
   :caption: JobMonitoring configuration for HTTPs

In any case, do not forget to update the URL of the service you just installed, such that other services can reach it.


Adding more tornado instances on a different machine
====================================================

Simply use ``dirac-install-component`` with no arguments on the new machine.


MasterCS special case
=====================

The master Configuration Server is different and needs to be run in a separate process. In order to do so:

* Do NOT specify ``Protocol=https`` in the service description, otherwise it will be ran with all the other Tornado services.
* If you run on the same machine as other TornadoService, specify a ``Port`` in the service description (you can keep the existing 9135, if already there).
* Modify the content of the Configuration so that URLs are updated to use HTTPs instead of DIPs.
* Add `HandlerPath = DIRAC/ConfigurationSystem/Service/TornadoConfigurationHandler.py` in the etc/dirac.cfg configuration file of the machine where the master CS is running (needed for bootstrap).

Finally, there is no automatic installations script. So just install a CS as you normally would do, and then edit the ``run`` file like that::

  #!/bin/bash
    rcfile=/opt/dirac/bashrc
    [ -e $rcfile ] && source $rcfile
    #
    export DIRAC_USE_TORNADO_IOLOOP=Yes
    exec 2>&1
    #
    [ "service" = "agent" ] && renice 20 -p $$
    #
    #
    exec tornado-start-CS -ddd
