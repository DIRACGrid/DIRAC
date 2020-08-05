=====================================
Managing Sites and Resources in DIRAC
=====================================

A Site, in DIRAC, is the entity that collects access points to resources that are related by locality in a functional sense,
i.e. the storage at a given Site is considered local to the CPU at the same Site and this relation will be used by DIRAC.
On the other hand, a Site must provide a single entry point responsible for the availability of the resources that it encompasses.
In the DIRAC sense, a Site can be from a fraction of physical computer center, to a whole regional grid.
It is the responsibility of the DIRAC administrator of the installation to properly define the sites.
Not all Sites need to grant access to all VOs supported in the DIRAC installation.

DIRAC can incorporate resources provided by existing Grid infrastructures (e.g. WLCG, OSG) as well as
sites not integrated in any grid infrastructure, but still
contributing with their computing and storage capacity, available as conventional clusters or file servers.

.. toctree::
   :maxdepth: 2

   catalog
   computingelements
   messagequeues
   storage
   agents2CS



Site Names
----------

In the :ref:`DIRAC configuration Sites <cs-site>` have names resulting from concatenation of the Domain prefix, the name of the Site and the country (or the funding body),
according to the ISO 3166 standard with a dot as a separator. 
The full DIRAC Site Name becomes of the form: [Domain].[Site].[co].
The full site names are used everywhere when the site resources are assigned to the context of a particular Domain:
in the accounting, monitoring, configuration of the Operations parameters, etc.

Examples of valid site names are:

* LCG.CERN.ch
* CLOUD.IN2P3.fr
* VAC.Manchester.uk
* DIRAC.farm.cern

The [Domain] may imply a (set of) technologies used for exploiting the resources, even though this is not necessarily true.
The use of these Domains is mostly for reporting purposes,
and it is the responsibility of the administrator of the DIRAC installation to chose them
in such a way that they are meaningful for the communities and for the computing resources served by the installation.
In any case, DIRAC will always be a default Domain if nothing else is specified for a given resource.

The Domain, Site and the country must be unique alphanumeric strings, irrespective of case, with a possible use of the following characters: "_".

Sites are providing access to the resources, therefore the /Resources/Sites section is the main place where the resources description is stored.
Resource types may include:

* Computing (via Computing Elements, "CE")
* Storage (via Storage Elements, "SE")
* Message Queues


The following sections will focus on other types of resources: Computing Elements (CEs), Storage Elements (SEs), Message
Queues (MQs), Catalogs


