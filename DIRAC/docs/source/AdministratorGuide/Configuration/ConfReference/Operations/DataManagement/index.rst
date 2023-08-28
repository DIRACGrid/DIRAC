.. _dirac-operations-dms:


Operations / DataManagement
=================================


* IgnoreMissingInFC (False): when removing a file/replica, trigger an error if the file is not on the SE
* UseCatalogPFN (True): when getting replicas with the DataManager, use the url stored in the catalog. If False, recalculate it
* SEsUsedForFailover ([]): SEs or SEGroups to be used as failover storages
* SEsNotToBeUsedForJobs ([]): SEs or SEGroups not to be used as input source for jobs
* SEsUsedForArchive ([]): SEs ir SEGroups to be used as Archive
* ForceSingleSitePerSE (True): return an error if an SE is associated to more than 1 site
* RegistrationProtocols (['srm', 'dips']): list of the possible protocols to be used to generate a SURL stored in the FileCatalog
* ThirdPartyProtocols (['srm']): list of the possible protocols to be used in replications
* AccessProtocols (['srm', 'dips']): list of the possible protocols to be used to perform the read operations and to get the space occupancy. Overwritten at the level of a StorageElement configuration.
* WriteProtocols (['srm', 'dips']): list of the possible protocols to be used to perform the write and remove operations. Overwritten at the level of a StorageElement configuration.
* AllowUserReplicaManagement (False): if set to True, clients without the FileCatalogManagement property can use the dirac-dms-remove-catalog-* commands to manipulate the file catalog.
* ForceIndexedMetadata (False): if True disables implicit creation of non-indexed Metadata.
* FTSPlacement section:

  - FTS3 section:

    - ServerPolicy (Random): policy to choose between FTS3 servers (Random, Sequence, Failover)

Read :ref:`multiProtocol` for more details on the meanings of RegistrationProtocols, ThirdPartyProtocols, AccessProtocols, and WriteProtocols
