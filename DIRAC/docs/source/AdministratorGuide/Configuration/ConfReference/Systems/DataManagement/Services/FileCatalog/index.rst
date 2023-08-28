Systems / DataManagement / <INSTANCE> / Service / FileCatalog - Sub-subsection
==============================================================================

FileCatalogHandler is a simple Replica and Metadata Catalog service. Special options are required to
configure this service, showed in the next table:

+--------------------+------------------------------------------------+------------------------------------------+
| **Name**           | **Description**                                | **Example**                              |
+--------------------+------------------------------------------------+------------------------------------------+
| *DefaultUmask*     | Default UMASK                                  | DefaultUmask = 509                       |
+--------------------+------------------------------------------------+------------------------------------------+
| *DirectoryManager* | Directory manager                              | DirectoryManager = DirectoryLevelTree    |
+--------------------+------------------------------------------------+------------------------------------------+
| *FileManager*      | File Manager                                   | FileManager = FileManager                |
+--------------------+------------------------------------------------+------------------------------------------+
| *GlobalReadAccess* | Boolean Global Read Access                     | GlobalReadAccess = True                  |
+--------------------+------------------------------------------------+------------------------------------------+
| *LFNPFNConvention* | Boolean indicating to use LFN PFN convention   | LFNPFNConvention = True                  |
+--------------------+------------------------------------------------+------------------------------------------+
| *SecurityManager*  | Security manager to be used                    | SecurityManager = NoSecurityManager      |
+--------------------+------------------------------------------------+------------------------------------------+
| *SEManager*        | Storage Element manager                        | SEManager = SEManagerDB                  |
+--------------------+------------------------------------------------+------------------------------------------+
| *ResolvePFN*       | Boolean indicating if resolve PFN must be done | ResolvePFN = True                        |
+--------------------+------------------------------------------------+------------------------------------------+
| *VisibleStatus*    | Visible Status                                 | VisibleStatus = AprioriGood              |
+--------------------+------------------------------------------------+------------------------------------------+
| *UniqueGUID*       | Use a unique GUID                              | UniqueGUID = False                       |
+--------------------+------------------------------------------------+------------------------------------------+
| *UserGroupManager* | User group manager                             | UserGroupManager = UserAndGroupManagerDB |
+--------------------+------------------------------------------------+------------------------------------------+
