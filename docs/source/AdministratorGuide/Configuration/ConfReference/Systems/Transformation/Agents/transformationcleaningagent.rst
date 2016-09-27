Systems / Transformation / <INSTANCE> / Agents / TransformationCleaningAgent - Sub-subsection
=============================================================================================

The TransformationCleaningAgent cleans up finalised (completed or cleaned) transformations

+------------------------------+----------------------------------------+------------------------------------+
| **Name**                     | **Description**                        | **Example**                        |
+------------------------------+----------------------------------------+------------------------------------+
| TransfIDMeta                 | MetaData key to use to identify        | TransfIDMeta=TransformationID      |
|                              | output data                            |                                    |
+------------------------------+----------------------------------------+------------------------------------+
| DirectoryLocations           | Location of the OutputData             | TransformationDB, MetadataCatalog  |
|                              |                                        |                                    |
+------------------------------+----------------------------------------+------------------------------------+
| ActiveSEs                    | From which SEs files will be removed   | []                                 |
|                              |                                        |                                    |
+------------------------------+----------------------------------------+------------------------------------+
| EnableFlag                   |                                        |  True/False                        |
|                              |                                        |                                    |
+------------------------------+----------------------------------------+------------------------------------+
| TransformationLogSE          | StorageElement holding log files       | LogSE                              |
|                              |                                        |                                    |
+------------------------------+----------------------------------------+------------------------------------+
| ArchiveAfter                 | How many days before archiving         | ArchiveAfter=7                     |
|                              | transformations                        |                                    |
+------------------------------+----------------------------------------+------------------------------------+
| shifterProxy                 | shifter to use to operations           | shifterProxy=DataManager           |
+------------------------------+----------------------------------------+------------------------------------+
