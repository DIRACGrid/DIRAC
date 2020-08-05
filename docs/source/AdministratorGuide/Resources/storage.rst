.. _resourcesStorageElement:

==============
StorageElement
==============



DIRAC provides an abstraction of a SE interface that allows to access different kind of them with a single interface. The access to each kind of SE ( SRMv2, DIRAC SE, ...) is achieved by using specific plugin modules that provide a common interface. The information necessary to define the proper plugin module and to properly configure this plugin to access a certain SE has to be introduced in the DIRAC :ref:`Configuration <dirac-cs-structure>`. An example of such configuration is::

    CERN-USER
    {
      OccupancyPlugin = WLCGAccountingJson
      OccupancyLFN = /lhcb/spaceReport.json
      SpaceReservation = LHCb_USER
      ReadAccess = Active
      WriteAccess = Active
      AccessProtocol.1
      {
        # The name of the DIRAC Plugin module to be used for implementation
        # of the access protocol
        PluginName = SRM2
        # Flag specifying the access type (local/remote)
        Access = remote
        # Protocol name
        Protocol = srm
        # Host endpoint
        Host = srm-lhcb.cern.ch
        Port = 8443
        # WSUrl part of the SRM-type PFNs
        WSUrl = /srm/managerv2?SFN=
        # Path to navigate to the VO namespace on the storage
        Path = /castor/cern.ch/grid
        # SRM space token
        SpaceToken = LHCb_USER
        # VO specific path definitions
        VOPath
        {
          biomed = /castor/cern.ch/biomed/grid
        }
      }
    }



Configuration options are:

* ``BackendType``: just used for information. No internal use at the moment
* ``SEType``: Can be `T0D1` or `T1D0` or `T1D1`. it is used to asses whether the SE is a tape SE or not. If the digit after `T` is `1`, then it is a tape.
* ``UseCatalogURL``: default `False`. If `True`, use the url stored in the catalog instead of regenerating it
* ``ChecksumType``: default `ADLER32`. NOT ACTIVE !
* ``Alias``: when set to the name of another storage element, it instanciates the other SE instead.
* ``ReadAccess``: default `True`. Allowed for Read if no RSS enabled (:ref:`activateRSS`)
* ``WriteAccess``: default `True`. Allowed for Write if no RSS enabled
* ``CheckAccess``: default `True`. Allowed for Check if no RSS enabled
* ``RemoveAccess``: default `True`. Allowed for Remove if no RSS enabled
* ``OccupancyLFN``: default (`/<vo>/occupancy.json`). LFN where the json file containing the space reporting is to be found
* ``OccupancyPlugin``: default (`empty`). Plugin to find the occupancy of a given storage.
* ``SpaceReservation``: just a name of a zone of the physical storage which can have some space reserved. Extends the SRM ``SpaceToken`` concept.

VO specific paths
-----------------

Storage Elements supporting multiple VO's can have definitions slightly differing with respect
to the `Path` used to navigate to the VO specific namespace in the physical storage. If a generic
`Path` can not be suitable for all the allowed VO's a `VOPath` section can be added to the Plugin
definition section as shown in the example above. In this section a specific `Path` can be defined for
each VO which needs it.


StorageElementBases
-------------------

Installations tend to have several StorageElements, with very similar configurations (e.g., the same Host and Port). It could be useful to factorize the SEs configuration to avoid repeating it.
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
  - RFIO (deprecated): for the rfio protocol.
  - Proxy: to be used with the StorageElementProxy.
  - S3: for S3 (e.g. AWS, CEPH) support (see :ref:`s3_support`)


There are also a set of plugins based on the gfal2 libraries (https://dmc.web.cern.ch/projects).

  - GFAL2_SRM2: for srm, replaces SRM2
  - GFAL2_XROOT: for xroot, replaces XROOT
  - GFAL2_HTTPS: for https
  - GFAL2_GSIFTP: for gsiftp


Default plugin options:

* `Access`: `Remote` or `Local`. If `Local`, then this protocol can be used only if we are running at the site to which the SE is associated. Typically, if a site mounts the storage as NFS, the `file` protocol can be used.


GRIDFTP Optimisation
^^^^^^^^^^^^^^^^^^^^

For efficiency reasons the environment variable ``DIRAC_GFAL_GRIDFTP_SESSION_REUSE`` should be exported in the server
``bashrc`` files::

  export DIRAC_GFAL_GRIDFTP_SESSION_REUSE=True

This enables the session reuse for the GRIDFTP plugin. This cannot be enabled generally because it can lead to denial
of service like attacks when thousands of jobs keep their connections to an SE alive for too long.


Space occupancy
---------------

Several methods allow to know how much space is left on a storage, depending on the protocol:

* dips: a simple system call returns the space left on the partition
* srm: the srm is able to return space occupancy based on the space token
* any other: a generic implementation has been made in order to retrieve a JSON file containing the necessary information.

A WLCG working group is trying to standardize the space reporting. So a standard will probably emerge soon (before 2053).
For the time being, we shall consider that the JSON file will contain a dictionary with keys `Total` and `Free` in Bytes.
For example::

   {
     "Total": 20,
     "Free": 10
   }

The LFN of this file is by default `/<vo>/occupancy.json`, but can be overwritten with the `OccupancyLFN` option of the SE.

The ``SpaceReservation`` option allows to specify a physical zone of the storage which would have space reservation (for example ``LHCb_USER``, ``LHCb_PROD``, etc). It extends the concept of ``SpaceToken`` that SRM has. This option is only used if the StoragePlugin does not return itself a ``SpaceReservation`` value.

The ``OccupancyPlugin`` allows to change the way space occupancy is measured. Several plugins are available (please refer to the module documentation):

* BDIIOccupancy: :py:mod:`~DIRAC.Resources.Storage.OccupancyPlugins.BDIIOccupancy`
* WLCGAccountingJson: :py:mod:`~DIRAC.Resources.Storage.OccupancyPlugins.WLCGAccountingJson`
* WLCGAccountingHTTPJson: :py:mod:`~DIRAC.Resources.Storage.OccupancyPlugins.WLCGAccountingHTTPJson` (likely to become the default in the future)



.. _multiProtocol:

Multi Protocol
--------------

There are several aspects of multi protocol:

  * One SE supports several protocols
  * SEs with different protocols need to interact
  * We want to use different protocols for different operations

DIRAC supports all of them. The bottom line is that before executing an action on an SE, we check among all the plugins defined for it, which plugins are the most suitable.
There are 4 Operation options under the `DataManagement` section used for that:

 * `RegistrationProtocols`: used to generate a URL that will be stored in the FileCatalog
 * `AccessProtocols`: used to perform the read operations
 * `WriteProtocols`: used to perform the write and remove operations
 * `ThirdPartyProtocols`: used in case of replications

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

External services like FTS requires pair of URLs to perform third party copy.
This is implemented using the same logic as described above. There is however an extra step: once the common protocols between 2 SEs have been filtered, an extra loop filter is done to make sure that the selected protocol can be used as read from the source and as write to the destination. Finally, the URLs which are returned are not necessarily the url of the common protocol, but are the native urls of the plugin that can accept/generate the common protocol. For example, if the common protocol is gsiftp but one of the SE has only an SRM plugin, then you will get an srm URL (which is compatible with gsiftp).


Protocol matrix
^^^^^^^^^^^^^^^

In order to make it easier to debug, the script :ref:`dirac-dms-protocol-matrix` will generate a CSV files that allows you to see what would happen if you were to try transfers between SEs

--------------------
StorageElementGroups
--------------------

StorageElements can be grouped together in a `StorageElementGroup`. This allows the systems or the users to refer to `any storage within this group`.
