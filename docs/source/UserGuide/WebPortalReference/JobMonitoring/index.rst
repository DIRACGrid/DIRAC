=========================
Job Monitoring
=========================

  This is part of DIRAC Web Portal project. For the description of the DIRAC Web Portal basic functionality look here.

    - `Description`_
    - `Selectors`_
    - `Columns`_
    - `Operations`_
    - `Actions`_


Description
===========

  The Job Monitoring is the most accessed page of the DIRAC Web Portal, provide information about User Jobs managed by the DIRAC Workload Management System. It shows details of the selected Jobs and allows certain Job selections.


Selectors
=========

  Selector widgets are provided in the  accordion menu left-side panel. These are drop-down lists with values that can be selected. A single or several values can be chosen. Once the selection is done press Submit button to refresh the contents of the table in the right-side panel. Use Reset button to clean up the values in all the selector widgets.

  The following selectors are available:

  **Site**

      The Pilot Job destination site using DIRAC nomenclature.

  **Status**

      Currently status of the job. The following values of status are possible:

      +--------------+----------------------------------------+
      | **Status**   | **Comment**                            |
      +--------------+----------------------------------------+
      | Waiting      | Job is accepted for DIRAC WMS          |
      +--------------+----------------------------------------+
      | Scheduled    | Job is assigned to a Site              |
      +--------------+----------------------------------------+
      | Running      | Job has started running in the CE      |
      +--------------+----------------------------------------+
      | Done         | Job finished successfully              |
      +--------------+----------------------------------------+
      |Deleted       | Job deleted by the user                |
      +--------------+----------------------------------------+
      |Killed        | Job killed by the user                 |
      +--------------+----------------------------------------+

  **Minor Status**

      Minor status complement the Job status, creating a complete sentence to have a better comprehension of the status.

      +---------------------------------+----------------------------------------------------------------------------------------+
      |**Minor Status**                 | **Comment**                                                                            |
      +---------------------------------+----------------------------------------------------------------------------------------+
      | Application Finished with Error | Job finished but with errors during application execution.                             |
      +---------------------------------+----------------------------------------------------------------------------------------+
      | Execution Complete              | Job successfully finished.                                                             |
      +---------------------------------+----------------------------------------------------------------------------------------+
      | Marked for Termination          | Job marked by the user for termination.                                                |
      +---------------------------------+----------------------------------------------------------------------------------------+
      | Maximum of Rescheduling reached | Job can be rescheduled a number of predefined times and this number was reached.       |
      +---------------------------------+----------------------------------------------------------------------------------------+
      | Pilot Agent Submission          | Job is Waiting until a pilot job being available.                                      |
      +---------------------------------+----------------------------------------------------------------------------------------+
      | Matched                         | Job is assigned to a pilot job.                                                        |
      +---------------------------------+----------------------------------------------------------------------------------------+

  **Application Status**

      With this information the user can know what kind of problem occurs during execution of the application.

      +---------------------------------+-----------------------------------------------------+
      | **Application Status**          |  **Comment**                                        |
      +---------------------------------+-----------------------------------------------------+
      |Failed Input Sandbox Download    |  Job failed to download Input Sandbox.              |
      +---------------------------------+-----------------------------------------------------+
      |Unknown                          |  Job failed by a unknown reason.                    |
      +---------------------------------+-----------------------------------------------------+

  **Owner**

      The Job Owner. This is the nickname corresponding to the Owner grid certificate distinguish name.

  **JobGroup**

      The Job Owner group using during job submission.

  **JobID**

      Number or list of numbers, of jobs selected.


Global Sort
===========

  This option is available in the accordion menu in the left panel. Allow users to sort jobs information showed in the right side panel. Available possibilities are:

    - JobID Ascending
    - JobID Descending
    - LastUpdate Ascending
    - LastUpdate Descending
    - Site Ascending
    - Site Descending
    - Status Ascending
    - Status Descending
    - Minor Status Ascending
    - Minor Status Descending

Current Statistics
==================

  This option is available in the accordion menu in the left panel, and show statistics of jobs selected, as status and number, in a table in the same panel. The columns presented are:

  **Status**

      Job status, in this case: Done, Failed, Killed, Waiting.

  **Number**

      Total number of jobs in the related status.

Global Statistics
=================

  This option is available in the accordion menu in the left panel, and show statistics of all of jobs **in the system**, as status and number, in a table in the same panel.

  **Status**

      Job status, in this case: Done, Failed, Killed, Waiting.

  **Number**

      Number of total jobs.

Columns
=======

  The information on the selected  Jobs is presented in the right-side panel in a form of a table. Note that not all the available columns are displayed by default. You can choose extra columns to display by choosing them in the menu activated by pressing on a menu button ( small triangle ) in any column title field.

  The following columns are provided:

  **JobID**

      JobID in DIRAC nomenclature.

  **Status**

      Job status.

      +-----------------+----------------------------------------------------------------+
      | **Status**      |     **Comment**                                                |
      +-----------------+----------------------------------------------------------------+
      | Waiting         |     Job is accepted for DIRAC WMS                              |
      +-----------------+----------------------------------------------------------------+
      | Scheduled       | Job is assigned to a pilot job to be executed.                 |
      +-----------------+----------------------------------------------------------------+
      | Running         | Job was started and is running into CE                         |
      +-----------------+----------------------------------------------------------------+
      | Done            | Job finished successfully                                      |
      +-----------------+----------------------------------------------------------------+
      | Deleted         | Job marked by the user for deletion                            |
      +-----------------+----------------------------------------------------------------+
      | Killed          | Job is marked for kill                                         |
      +-----------------+----------------------------------------------------------------+

  **Minor Status**

      Complement Job Status.

      +---------------------------------+-----------------------------------------------------+
      |     **Minor Status**            | **Comment**                                         |
      +=================================+=====================================================+
      | Application Finished with Error | Job finished but with errors during execution.      |
      +---------------------------------+-----------------------------------------------------+
      | Execution Complete              | Job successfully finished.                          |
      +---------------------------------+-----------------------------------------------------+
      | Marked for Termination          | Job marked by the user for termination.             |
      +---------------------------------+-----------------------------------------------------+
      | Maximun of Rescheduling reached | Job can be rescheduled a number of predefined times.|
      +---------------------------------+-----------------------------------------------------+
      | Pilot Agent Submission          | Job is Waiting until a pilot job be available.      |
      +---------------------------------+-----------------------------------------------------+
      | Matched                         | Job is assigned to a pilot job.                     |
      +---------------------------------+-----------------------------------------------------+

  **Application Status.**

  **Site**

      The Job destination site in DIRAC nomenclature.

  **JobName**

      Job Name assigned by the User.

  **Owner**

      Job Owner. This is the nickname of the Job Owner corresponding to the users certificate distinguish name.

  **LastUpdateTime**

      Job last status update time stamp (UTC)

  **LastSingofLife**

      Time stamp (UTC) of last sign of life of the Job.

  **SubmissionTime**

      Time stamp (UTC) when the job was submitted.

Operations
==========

  Clicking on the line corresponding to a Job, one can obtain a menu which allows certain operations on the Job. Currently, the following operations are available.

   **JDL**

      Job JDL into DIRAC nomenclature.

   **Attributes**

      Job Attributes associated with the job, owner, priority, etc.

   **Parameters**

       Parameters of the site where the job ran or is running.

   **LoggingInfo**

       Get Job information in a pop-up panel about each status where the job has been.

   **PeekStandartOutput**

       Get the standard output of the  Job in a pop-up panel.

   **GetLogFile**


   **GetPendingRequest**


   **GetStagerReport**


   **GetSandboxFile**

Actions
=======

  Actions that the user can perform over their jobs are showed below:

    +-----------+---------------------------+
    |**Action** |  **Comment**              |
    +-----------+---------------------------+
    | Reset     | Restart the Job           |
    +-----------+---------------------------+
    | Kill      | Kill the Job selected     |
    +-----------+---------------------------+
    | Delete    | Delete the job            |
    +-----------+---------------------------+
