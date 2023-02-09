.. _https_services:

==================================
Installing HTTPs services in DIRAC
==================================

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
      Tornado = Production
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

This procedure is to be used if you are want to serve a new service with HTTPs.


Case 1: you do NOT run the equivalent DISET service
---------------------------------------------------

This is the most trivial case. Just run ``dirac-install-tornado-service`` with the service you are interested in. This will install an ``runit`` component running ``tornado-start-all``.

Case 2: you run the equivalent DISET service
--------------------------------------------

Because the CS already contains the handler definition for DISET, ``dirac-install-tornado-service`` will not modify it. Thus, you have to update it yourself, before running the command, otherwise ``tornado-start-all`` will not find any service to run, and the installation will be shown as failed.

Procedure:

#. Update by hand the CS of the desired service:

  * Remove the port definition
  * Modify the handler to point to the Tornado handler
  * add ``Protocol=https``

The example the follows is for the "DIRAC File Catalog" (DFC) service. This would normally be in CS as::

  Systems
  {
    ...
    DataManagement
    {
      Production
      {
        URLs
        {
          ...
          FileCatalog = dips://my.server.org:9197/DataManagement/FileCatalog
        }
        Services
        {
          ...
          FileCatalog
          {
            ...
            Port = 9197
            Protocol = dips
            ...
          }
        }
        ...
      }
    }
  }


And you need to change it to::

  Systems
  {
    ...
    DataManagement
    {
      Production
      {
        URLs
        {
          ...
          FileCatalog = https://my.server.org:8443/DataManagement/FileCatalog
        }
        Services
        {
          ...
          FileCatalog
          {
            ...
            Protocol = https
            HandlerPath = DIRAC/DataManagementSystem/Service/TornadoFileCatalogHandler.py
            ...
          }
        }
        ...
      }
    }
  }


#. Run ``dirac-install-tornado-service`` or restart the tornado component if already running.

.. note::
  This means that from now on, the DISET service cannot be restarted anymore, as its configuration would be wrong.
  So, go ahead and remove (by hand, for now) the `runit`-related directories of the DISET service.

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

Simply use ``dirac-install-tornado-service`` with no arguments on the new machine.


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
