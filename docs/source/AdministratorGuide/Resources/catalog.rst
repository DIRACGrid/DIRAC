.. _resourcesCatalog:

========
Catalogs
========

Catalogs represent the namespace in DIRAC. They are queried based on the LFN. Even if one is used as a reference (see `Master catalog`), you can use several catalogs in parallel. Every catalog has read and write methods.

The definition of catalogs is shared between two sections:

* `/Resources/FileCatalogs`: this describes the catalog, how to access it, and all its options
* `/Operations/<vo/setup>/Services/Catalogs/`: this describes how we use the catalog.

Resources
---------

Every catalogs should be defined in the `/Resources/FileCatalogs` section. You define one section per catalog. This section is supposed to describe how to access the catalog::

  <catalogName>
  {
    CatalogType = <myCatalogType>
    CatalogURL = <myCatalogURL>
    <anyOption>
  }


* CatalogType: default `<catalogName>`

used to load the plugin located in `Resources.Catalog.<catalogType>Client`

* CatalogURL default `DataManagement/<CatalogType>`

  passed as `url` argument to the plugin in case it is an RPCClient

* <anyOption>

passed as keyed arguments to the constructor of your plugin.

For example::

   Resources
   {
      FileCatalogs
      {
        FileCatalog
        {
        }
        # This is not in DIRAC, just
        # another catalog
        BookkeepingDB
        {
          CatalogURL = Bookkeeping/BookkeepingManager
        }
      }
   }



Operations
----------

First of all, `/Operations/<vo/setup>/Services/Catalogs/CatalogList` defines which catalogs are eligible for use. If this is not defined, we consider that all the catalogs defined under `/Operations/<vo/setup>/Services/Catalogs/` are eligible.

Then, each catalog should have a few (case-sensitive) options defined:

* `Status`: (default `Active`). If anything else than `Active`, the catalog will not be used
* `AccessType`: `Read`/`Write`/`Read-Write`. No default, must be defined. This defines if the catalog is read-only, write only or both.
* `Master`: see :ref:`masterCatalog`

For example::


   Catalogs
   {

      FileCatalog
      {
        AccessType = Read-Write
        Status = Active
        Master = True
      }
      # This is not in DIRAC, just
      # another catalog
      BookkeepingDB
      {
        AccessType = Write
        Status = Active
      }
   }

.. _masterCatalog:

Master catalog
--------------

When there are several catalogs, the write operations are not atomic anymore: the master catalog then becomes the reference. Any write operation is first attempted on the master catalog. If it fails, the operation is considered failed, and no attempt is done on the others. If it succedes, the other catalogs will be attempted as well, but a failure in one of the secondary catalogs is not considered as a complete failure.
Of course, there should be only one master catalog

Conditional FileCatalogs
------------------------

The `Status` and `AccessType` flags are global and binary. However it is sometimes a desirable feature to activate a catalog under some conditions only. This is what the conditional FCs are about. Conditions are evaluated for every catalog at every call and for every file. Conditions are defined in a section `Operation/<vo/setup>/Services/Catalogs/<CatalogName>/Conditions/`. They are evaluated by plugins, so it is very modular.

In this section, you can create an CS option for every method of your catalog. The name of the option should be the method name, and the value should be the condition to evaluate. If there are no condition defined for a given method, we check the global `READ`/`WRITE` condition, which are used for all read/write methods. If this does not exist either, we check the global `ALL` condition. If there are no condition at all, everything is allowed.

The conditions are expressed as boolean logic, where the basic bloc has the form `pluginName=whateverThatWillBePassedToThePlugin`. The basic blocs will be evaluated by the respective plugins, and the result can be combined using the standard boolean operators::

  * ! for not
  * & for and
  * \| for or
  * [ ] for prioritizing the operations

All these characters, as well as the '=' symbol cannot be used in any expression to be evaluated by a plugin.

Example of rules are::

  * Filename=startswith('/lhcb') & Proxy=voms.has(/lhcb/Role->production)
  * [Filename=startswith('/lhcb') & !Filename=find('/user/')] | Proxy=group.in(lhcb_mc, lhcb_data)

The current plugins are:

* Filename: evaluation done on the LFN (:py:class:`~DIRAC.Resources.Catalog.ConditionPlugins.FilenamePlugin.FilenamePlugin`)
* Proxy: evaluation done on the attributes of the proxy (user, group, VOMS role, etc) (:py:class:`~DIRAC.Resources.Catalog.ConditionPlugins.ProxyPlugin.ProxyPlugin`)
