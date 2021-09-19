=============================
Transformation Monitor
=============================

  This is part of DIRAC Web Portal project. For the description of the DIRAC Web Portal basic functionality look here.

    - `Description`_
    - `Selectors`_
    - `Current Statistics`_
    - `Global Statistics`_
    - `Columns`_
    - `Operations`_



Description
===========

Transformation Monitoring provides information about Productions managed by the DIRAC **Workload Management System**
and data replication/removals managed by the DIRAC **Data Management System**.
It shows details of the selected production and allows users to refine certain selections.


Selectors
=========

  Selector widgets are provided in the left-side panel. These are drop-down lists with values that can be selected. A single or several values can be chosen. Once the selection is done press Submit button to refresh the contents of the table in the right-side panel. Use Reset button to clean up the values in all the selector widgets.

  **Status**

       Allow select production depending of status, the possible status of selections are:

       ======================   ================================================================================
         **Status**                **Comments**
       ----------------------   --------------------------------------------------------------------------------
         New                       New Production
         Active                    Active Production
         Stopped                   A production can be stopped by
         Validating Input          Inputs of production are being checked
         Validating Output         Outputs of productions are being checked
         Waiting Integrity         **The system is waiting for integrity results??**
         Remove Files
         Removed Files
         Completed                 Production completely processed
         Archived                  **Output of production are archived into**
         Cleaning                  Production is being cleaned
       ======================   ================================================================================


Current Statistics
==================

  This option is available in the left panel, shows production statistics based on currently selected productions, resultant information is showed in a table in the same panel.

Global Statistics
=================

  This option is available in the left panel, and shows global statistics about all productions in a table in the same panel.

Columns
=======

  The information on the selected productions is presented in the right-side panel in a form of a table. Note that not all the available columns are displayed by default. You can choose extra columns to display by choosing them in the menu activated by pressing on a menu button ( small triangle ) in any column title field.

   **ID**

       DIRAC Production ID.

   **Status**

       Production Status.

   **Agent Type**

       How the agent was submit: Automatic or Manual

   **Type**

       Production Type, by example: MCSimulation.

   **Group**

       DIRAC group of the user than submit the production.

   **Name**

       Production name.

   **Files**

       Number of files required to run the production.

   **Processed(%)**

       Percentage of completeness of the production. It can be 0 in case the production can be extended.

   **Files Processed**

       Number of files processed until now.

   **Files Assigned**

       Number of files to be processed.

   **Files Problematic**

        **??**

   **Files Unused**

       Number of failed files in case production fail, it was sent but not processed.

   **Created**

       Number of jobs created to run the production.

   **Submitted**

       Number of jobs submitted to different sites.

   **Waiting**

       Number of jobs in status waiting.

   **Running**

       Number of jobs running.

   **Done**

       Number of jobs in status done.

   **Failed**

       Number of jobs failed.

   **Stalled**

       Number of jobs stalled.

   **InheritedFrom**

        **?? production ID**

   **GroupSize**

   **FileMask**

   **Plugin**

   **EventsPerJob**

   **MaxNumberOfJobs**

       Maximum number of jobs to be summited for the selected production.

Operations
==========

  Clicking on the line corresponding to a Production, one can obtain a menu which allows certain operations on the production. Currently, the following operations are available.

  **Show Jobs**

      Show associated jobs with the selected production.

  **LoggingInfo**

      Show logging info for the selected production.

  **FileStatus**


  **Show Details**

      Details about the production selected

  **Actions**

     Actions can be done using the selectors and buttons in the title field, the options are:

     =========== ================================
        Action          Comment
     ----------- --------------------------------
        Start       Start the production
        Stop        Stop the production
        Flush       Flush the production
        Clean       Clean
     =========== ================================

  **Show Value**

      Show value of selected cell.
