.. _rss_installation:

============
Installation
============

This page describes the basic steps to install, configure, activate and start using the ResourceStatus system of DIRAC.

*WARNING*: If you have doubts about the success of any step, DO NOT ACTIVATE RSS.

*WARNING*: REPORT FIRST to the DIRAC FORUM !

----------------
CS Configuration
----------------

The configuration for RSS sits under the following path on the CS following the usual /Operations section convention::

    /Operations/[Defaults|SetupName]/ResourceStatus

Please, make sure you have the following schema::

    /Operations/[Defaults|SetupName]/ResourceStatus
      /Config
        State        = InActive
        Cache        = 300
        /StatusTypes
            default = all
            StorageElement = ReadAccess,WriteAccess,CheckAccess,RemoveAccess

For a more detailed explanation, take a look to the official documentation:
:ref:`rss-configuration`.

---------
Fresh DB
---------

Needs a fresh DB installation. `ResourceStatusDB` and `ResourceManagementDB` are
needed. Information on former ResourceStatusDB can be discarded. Delete the old
database tables. If there is no old database, just install a new one, either
using the dirac-admin-sysadmin-cli or directly from the machine as follows::

    $ dirac-install-db ResourceStatusDB
    $ dirac-install-db ResourceManagementDB

------------------
Generate DB tables
------------------

The DB tables will be created when the services are started for the first time.

--------------
Run service(s)
--------------

RSS - basic - needs the following services to be up and running:
ResourceStatus/ResourceStatus, ResourceStatus/ResourceManagement
please install them using the dirac-admin-sysadmin-cli command, and make sure it
is running.::

  install service ResourceStatus ResourceManagement
  install service ResourceStatus ResourceStatus
  install service ResourceStatus Publisher

In case of any errors, check that you have the information about DataBase 'Host' in the configuration file.

The host(s) running the RSS services or agents need the 'SiteManager' property.

.. _rss_populate:

---------------
Populate tables
---------------

First check that your user has 'SiteManager' privilege, otherwise it will be "Unauthorized query" error.
Let's do it one by one to make it easier::

    $ dirac-rss-sync --element Site -o LogLevel=VERBOSE
    $ dirac-rss-sync --element Resource -o LogLevel=VERBOSE
    $ dirac-rss-sync --element Node -o LogLevel=VERBOSE

---------------------------------------
Initialize Statuses for StorageElements
---------------------------------------

Copy over the values that we had on the CS for the StorageElements::

    $ dirac-rss-sync --init -o LogLevel=VERBOSE

*WARNING*: If the StorageElement does not have a particular StatusType declared

*WARNING*: on the CS, this script will set it to Banned. If that happens, you will

*WARNING*: have to issue the dirac-rss-status script over the elements that need

*WARNING*: to be fixed.


--------------------
Set statuses by HAND
--------------------

In case you entered the WARNING ! on point 4, you may need to identify the
status of your StorageElements. Try to detect the Banned SEs using the
following::

    $ dirac-rss-list-status --element Resource --elementType StorageElement --status Banned

If is there any SE to be modified, you can do it as follows::

    $ dirac-rss-set-status --element Resource --name CERN-USER --statusType ReadAccess --status Active --reason "Why not?"
    # This matches all StatusTypes
    $ dirac-rss-set-status --element Resource --name CERN-USER --status Active --reason "Why not?"

.. _activateRSS:

------------
Activate RSS
------------

If you did not see any problem, activate RSS by setting the CS option::

    /Operations/[Defaults|SetupName]/ResourceStatus/Config/State = Active

------
Agents
------

The agents that are required:

    - CacheFeederAgent
    - SummarizeLogsAgent

The following agents are also necessary, but they won't do nothing until some policies are defined in the CS.
The policy definitions is explained in :ref:`rss_advanced_configuration` ::

    - ElementInspectorAgent
    - SiteInspectorAgent
    - TokenAgent
    - EmailAgent

Please, install them and make sure they are up and running. The configuration of these agents can be found :mod:`Here <ResourceStatusSystem.Agent>`.
