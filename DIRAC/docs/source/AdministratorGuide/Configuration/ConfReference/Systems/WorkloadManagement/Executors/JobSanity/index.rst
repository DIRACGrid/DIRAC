Systems / WorkloadManagement / <INSTANCE> / Executors / JobSanity - Sub-subsection
====================================================================================

The JobSanity executor screens jobs for the following problems
   - Problematic JDL
   - Jobs with too much input data e.g. > 100 files
   - Jobs with input data incorrectly specified e.g. castor:/
   - Input sandbox not correctly uploaded.
   - Output data already exists (not implemented)

+---------------------+---------------------------------------+--------------------------------------------+
| **Name**            | **Description**                       | **Example**                                |
+---------------------+---------------------------------------+--------------------------------------------+
| *InputDataCheck*    | Boolean, check  if input data is prop-| InputDataCheck = True                      |
|                     | erly formated, default=True           |                                            |
+---------------------+---------------------------------------+--------------------------------------------+
| *MaxInputDataPerJob*| Integer, Maximum number of input lfns | MaxInputDataPerJob=100                     |
|                     |                                       |                                            |
+---------------------+---------------------------------------+--------------------------------------------+
| *InputSandboxCheck* | Check for input sandbox files         | InputSandboxCheck = True                   |
|                     |                                       |                                            |
+---------------------+---------------------------------------+--------------------------------------------+
| *OutputDataCheck*   | Check if output data exists           | OutputDataCheck = True                     |
|                     | Not Implemented                       |                                            |
+---------------------+---------------------------------------+--------------------------------------------+
