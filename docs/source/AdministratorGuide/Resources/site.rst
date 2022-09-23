.. _cs-site:

Sites
=====


Site Names
----------

Sites have names resulting from concatenation of:

- Domain: Grid site name, expressed in uppercase, for example: LCG, EELA
- Site: Institution acronym in uppercase, for example: CPPM
- Country: country where the site is located, expressed in lowercase, for example fr


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



Configuration
-------------

.. literalinclude:: /dirac.cfg
    :start-after: ##BEGIN SiteConfiguration
    :end-before: ##
    :dedent: 4
    :caption: Site configuration
