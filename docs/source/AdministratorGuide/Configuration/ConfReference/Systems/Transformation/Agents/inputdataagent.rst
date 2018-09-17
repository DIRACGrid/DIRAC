Systems / Transformation / <INSTANCE> / Agents / InputDataAgent - Sub-subsection
================================================================================

The InputDataAgent updates the transformation files of active transformations
given an InputDataQuery fetched from the Transformation Service.

Possibility to speedup the query time by only fetching files that were added since the last iteration.
Use the CS option RefreshOnly (False by default) and set the DateKey (empty by default) to the meta data
key set in the DIRAC FileCatalog.

This Agent also reads some options from Operations/Transformations:

* DataProcessing
* DataManipulation
* ExtendableTransfTypes


+------------------------------+-------------------------------------+------------------------------+
| **Name**                     | **Description**                     | **Example**                  |
+------------------------------+-------------------------------------+------------------------------+
| FullUpdatePeriod             | Time after a full update will be    | 86400                        |
|                              | done                                |                              |
+------------------------------+-------------------------------------+------------------------------+
| RefreshOnly                  | Only refresh new files, needs       | False                        |
|                              | the DateKey                         |                              |
+------------------------------+-------------------------------------+------------------------------+
| DateKey                      | Meta data key for file              |                              |
|                              | creation date                       |                              |
+------------------------------+-------------------------------------+------------------------------+
| TransformationTypes          | TransformationTypes to handle       |                              |
|                              | in this agent instance              |                              |
+------------------------------+-------------------------------------+------------------------------+
