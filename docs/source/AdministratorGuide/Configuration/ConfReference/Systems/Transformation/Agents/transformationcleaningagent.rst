Systems / Transformation / <INSTANCE> / Agents / TransformationCleaningAgent - Sub-subsection
=============================================================================================

The TransformationCleaningAgent cleans up finalised (completed or cleaned) transformations

+------------------------------+----------------------------------------+------------------------------------+
| **Name**                     | **Description**                        | **Example**                        |
+------------------------------+----------------------------------------+------------------------------------+
| DirectoryLocations           | Location of the OutputData             | TransformationDB, MetadataCatalog  |
|                              |                                        |                                    |
+------------------------------+----------------------------------------+------------------------------------+
| ActiveSEs                    | From which SEs files will be removed   | [CERN-Disk, IN2P3-DST]             |
|                              | If empty or not existing, only         | Default: []                        |
|                              | files in the Catalog will be removed   |                                    |
+------------------------------+----------------------------------------+------------------------------------+
| EnableFlag                   | Do something or not?                   |  True/False                        |
|                              |                                        |  Default: True                     |
+------------------------------+----------------------------------------+------------------------------------+
| ArchiveAfter                 | How many days to wait before archiving | 14                                 |
|                              | transformations                        | Default: 7                         |
+------------------------------+----------------------------------------+------------------------------------+
| shifterProxy                 | shifter to use for removal operations  | DataManager (also the default)     |
+------------------------------+----------------------------------------+------------------------------------+
