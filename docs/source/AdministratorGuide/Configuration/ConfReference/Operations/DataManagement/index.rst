.. _dirac-operations-dms:


Operations / DataManagement
=================================


* IgnoreMissingInFC (False): when removing a file/replica, trigger an error if the file is not on the SE
* UseCatalogPFN (True): when getting replicas with the DataManager, use the url stored in the catalog. If False, recalculate it
* SEsUsedForFailover ([]): SEs or SEGroups to be used as failover storages
* SEsNotToBeUsedForJobs ([]): SEs or SEGroups not to be used as input source for jobs
* SEsUsedForArchive ([]): SEs ir SEGroups to be used as Archive
* ForceSingleSitePerSE (True): return an error if an SE is associated to more than 1 site
* FTSVersion (FTS2): version of FTS to use. Possibilities: FTS3 or FTS2 (deprecated)
* FTSPlacement section:

  - FTS2 section: deprecated
  - FTS3 section:

    - ServerPolicy (Random): policy to choose between FTS3 servers (Random, Sequence, Failover)
