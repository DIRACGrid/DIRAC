.. _resourcesStorageElement:

---------------
StorageElements
---------------



DIRAC provides an abstraction of a SE interface that allows to access different kind of them with a single interface. The access to each kind of SE ( SRMv2, DIRAC SE, ...) is achieved by using specific plugin modules that provide a common interface. The information necessary to define the proper plugin module and to properly configure this plugin to access a certain SE has to be introduced in the DIRAC :ref:`Configuration <dirac-cs-structure>`. An example of such configuration is::

    CERN-USER
    {
      ReadAccess = Active
      WriteAccess = Active
      AccessProtocol.1
      {
        PluginName = SRM2
        Access = remote
        Protocol = srm
        Host = srm-lhcb.cern.ch
        Port = 8443
        WSUrl = /srm/managerv2?SFN=
        Path = /castor/cern.ch/grid
        SpaceToken = LHCb_USER
      }
    }



Configuration options are:

* `BackendType`: just used for information. No internal use at the moment
* `SEType`: Can be `T0D1` or `T1D0` or `T1D1`. it is used to asses whether the SE is a tape SE or not. If the digit after `T` is `1`, then it is a tape.
* `UseCatalogURL`: default `False`. If `True`, use the url stored in the catalog instead of regenerating it
* `ChecksumType`: default `ADLER32`. NOT ACTIVE !
* `Alias`: when set to the name of another storage element, it instanciates the other SE instead.
* `ReadAccess`: default `True`. Allowed for Read if no RSS enabled
* `WriteAccess`: default `True`. Allowed for Write if no RSS enabled
* `CheckAccess`: default `True`. Allowed for Check if no RSS enabled
* `RemoveAccess`: default `True`. Allowed for Remove if no RSS enabled



-------------------
StorageElementBases
-------------------

In order to factorize the configuration, it is possible to use `BaseSE`, which acts just like inheritance in object programming. You define a SE just like any other but in the `StorageElementBases` section. This SE can then be refered to by another SE. This new SE will inherit all the configuration from its parents, and can override it.  For example::

    StorageElementBases
    {
      CERN-EOS
      {
        BackendType = Eos
        SEType = T0D1
        AccessProtocol.1
        {
          Host = srm-eoslhcb.cern.ch
          Port = 8443
          PluginName = GFAL2_SRM2
          Protocol = srm
          Path = /eos/lhcb/grid/prod
          Access = remote
          SpaceToken = LHCb-EOS
          WSUrl = /srm/v2/server?SFN=
        }
      }
    }
    StorageElements
    {
      CERN-DST-EOS
      {
        BaseSE = CERN-EOS
      }
      CERN-USER
      {
        BaseSE = CERN-EOS
        PledgedSpace = 205
        AccessProtocol.1
        {
          PluginName = GFAL2_SRM2
          Path = /eos/lhcb/grid/user
          SpaceToken = LHCb_USER
        }
      }
      GFAL2_XROOT
      {
        Host = eoslhcb.cern.ch
        Port = 8443
        Protocol = root
        Path = /eos/lhcb/grid/user
        Access = remote
        SpaceToken = LHCb-EOS
        WSUrl = /srm/v2/server?SFN=
      }
    }


This definition would be strictly equivalent to::

    StorageElementBases
    {
      CERN-EOS
      {
        BackendType = Eos
        SEType = T0D1
        AccessProtocol.1
        {
          Host = srm-eoslhcb.cern.ch
          Port = 8443
          PluginName = GFAL2_SRM2
          Protocol = srm
          Path = /eos/lhcb/grid/prod
          Access = remote
          SpaceToken = LHCb-EOS
          WSUrl = /srm/v2/server?SFN=
        }
      }
    }
    StorageElements
    {
      CERN-DST-EOS
      {
        BackendType = Eos
        SEType = T0D1
        AccessProtocol.1
        {
          Host = srm-eoslhcb.cern.ch
          Port = 8443
          PluginName = GFAL2_SRM2
          Protocol = srm
          Path = /eos/lhcb/grid/prod
          Access = remote
          SpaceToken = LHCb-EOS
          WSUrl = /srm/v2/server?SFN=
        }
      }
      CERN-USER
      {
        BackendType = Eos
        SEType = T0D1
        PledgedSpace = 205
        AccessProtocol.1
        {
          Host = srm-eoslhcb.cern.ch
          Port = 8443
          PluginName = GFAL2_SRM2
          Protocol = srm
          Path = /eos/lhcb/grid/user
          Access = remote
          SpaceToken = LHCb_USER
          WSUrl = /srm/v2/server?SFN=
        }
      }
      GFAL2_XROOT
      {
        Host = eoslhcb.cern.ch
        Port = 8443
        PluginName =  GFAL2_XROOT
        Protocol = root
        Path = /eos/lhcb/grid/user
        Access = remote
        SpaceToken = LHCb-EOS
        WSUrl = /srm/v2/server?SFN=
      }
    }

Note that base SE must be separated from the inherited SE in two different sections. You can also notice that the name of the protocol section can be a plugin name. In this way, you do not need to specify a plugin name inside.




Available protocol plugins
--------------------------

DIRAC comes with a bunch of plugins that you can use to interact with StorageElements.
These are the plugins that you should define in the `PluginName` option of your StorageElement definition.

  - DIP: used for dips, the DIRAC custom protocol (useful for example for DIRAC SEs).
  - File: offers an abstraction of the local access as an SE.
  - SRM2 (deprecated): for the srm protocol, using the deprecated gfal libraries.
  - RFIO (deprecated): for the rfio protocol.
  - Proxy: to be used with the StorageElementProxy.
  - XROOT: for the xroot protocol, using the python xroot binding (http://xrootd.org/doc/python/xrootd-python-0.1.0/#).

There are also a set of plugins based on the gfal2 libraries (https://dmc.web.cern.ch/projects).

  - GFAL2_SRM2: for srm, replaces SRM2
  - GFAL2_XROOT: for xroot, replaces XROOT
  - GFAL2_HTTPS: for https
  - GFAL2_GSIFTP: for gsiftp


Default plugin options:

* `Access`: `Remote` or `local`.


Multi Protocol
--------------

There are several aspects of multi protocol:
  - One SE supports several protocols
  - SEs with different protocols need to interact
  - We want to use different protocols for different operations

DIRAC supports all of them. The bottom line is that before executing an action on an SE, we check among all the plugins defined for it, which plugins are the most suitable.
There are 4 Operation options under the `DataManagement` section used for that:

 - `RegistrationProtocols`: used to generate a URL that will be stored in the FileCatalog
 - `AccessProtocols`: used to perform the read operations
 - `WriteProtocols`: used to perform the write and remove operations
 - `ThirdPartyProtocols`: used in case of replications

When performing an action on an SE, the StorageElement class will evaluate, based on these lists, and following this preference order, which StoragePlugins to use.
The behavior is straightforward for simple read or write actions. It is however a bit more tricky when it comes to third party copies.

Each StoragePlugins has a list of protocols that it is able to accept as input and a list that it is able to generate. In most of the cases, for protocol X, the plugin
is able to generate URL for the protocol X, and to take as input URL for the protocol X and local files. There are plugins that can do more, like GFAL2_SRM2 plugins
that can handle many more (xroot, gsiftp, etc). It may happen that the SE can be writable only by one of the protocol. Suppose the following situation: you want to replicate
from storage A to storage B. Both of them have as plugins GFAL2_XROOT and GFAL2_SRM2; AccessProtocols is "root,srm", WriteProtocols is "srm" and ThirdPartyProtocols is "root,srm".
The negociation between the storages to find common protocol for third party copy will lead to "root,srm". Since we follow the order, the sourceURL will be a root url,
and it will be generated by GFAL2_XROOT because root is its native protocol (so we avoid asking the srm server for a root turl). The destination will only consider using
GFAL2_SRM2 plugins because only srm is allowed as a write plugin, but since this plugins can take root URL as input, the copy will work.


The WriteProtocols and AccessProtocols list can be locally overwritten in the SE definition.

Multi Protocol with FTS
^^^^^^^^^^^^^^^^^^^^^^^^

CAUTION: not yet active

External services like FTS requires pair of URLs to perform third party copy.
This is implemented using the same logic as described above. There is however an extra step: once the common protocols between 2 SEs have been filtered, an extra loop filter is done to make sure that the selected protocol can be used as read from the source and as write to the destination. Finally, the URLs which are returned are not necessarily the url of the common protocol, but are the native urls of the plugin that can accept/generate the common protocol. For example, if the common protocol is gsiftp but one of the SE has only an SRM plugin, then you will get an srm URL (which is compatible with gsiftp).


--------------------
StorageElementGroups
--------------------

StorageElements can be grouped together in a `StorageElementGroup`. This allows the systems or the users to refer to `any storage within this group`.
