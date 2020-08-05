.. _dirac-systems-cs:

Systems configuration
=======================

Each DIRAC system has its corresponding section in the Configuration namespace.

.. note:: The configuration options for services and agents are being moved to the :ref:`Code
          Documentation <code_documentation>`. You can find the options for each service and agent
          on the individual documentation page of the respective agent or service.

.. toctree::
   :maxdepth: 1

   Accounting/index
   Configuration/index
   DataManagement/index
   WorkloadManagement/index
   /CodeDocumentation/RequestManagementSystem/RequestManagementSystem_Module
   Framework/index
   StorageManagement/index
   Transformation/index


Default structure
-----------------

In each system, per setup, you normally find the following sections:

* Agents: definition of each agent
* Services: definition of each service
* Databases: definition of each db
* URLs: Resolution of the URL of a given Service (like 'DataManagement/FileCatalog') to a list of real urls (like 'dips://<host>:<port>/DataManagement/FileCatalog'). They are tried in a random order.
* FailoverURLs: Like URLs, but they are only tried if no server in URLs was successfully contacted.


Main Servers
------------

There might be setup in which all services are installed behind one or several dns alias(es) or gateways (typically orchestrator like Mesos/Kubernetes). When this is the case, it can be bothering to redefine the very same URL everywhere, especially the day the machine name changes.

For this reason, there is the possibility to define a entry in the Operation section which contains the list of servers:

.. code-block:: guess

  Operations/<Setup>/MainServers = server1, server2


There should be no port, no protocol. In the system configuration, one can then write:

.. code-block:: guess

  System
  {
    URLs
    {
      Service = dips://$MAINSERVERS$:1234/System/Service
    }
  }

This will resolve in the following 2 urls:

.. code-block:: guess

  dips://server1:1234/System/Service, dips://server2:1234/System/Service


Using together the FailoverURLs section, it can be interesting for orchestrator's setup, where there is a risk for the whole cluster to go down:

.. code-block:: guess

  System
  {
    URLs
    {
      Service = dips://$MAINSERVERS$:1234/System/Service
    }
    FailoverURLs
    {
      Service = dips://failover1:1234/System/Service,dips://failover2:1234/System/Service
    }
  }
  Operations
  {
    Defaults
    {
      MainServers = gateway1, gateway2
    }
  }


This results in all calls going to gateway1 and gateway2, which could be frontend to your orchestrator, and only if none of them answers, then do we use failover1 and failover2, which can be installed on separate machines, independent from the orchestrator
