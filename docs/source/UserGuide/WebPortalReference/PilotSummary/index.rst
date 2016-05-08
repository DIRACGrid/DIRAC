=========================
Pilot Summary
=========================

  This is part of DIRAC Web Portal project. For the description of the DIRAC Web Portal basic functionality look here.


    - `Description`_
    - `Selectors`_
    - `Statistics`_
    - `Columns`_
    - `Operations`_


Description
===========

  Pilot summary present a table with statistics of all pilots assigned by sites and sites efficiency this information give to the user the possibility to choose sites to submit their jobs according this values. This service is currently managed by the DIRAC Workload Management System.

Selectors
=========

  Selector widgets are provided in the left-side panel. These are drop-down lists with values that can be selected. A single or several values can be chosen. Once the selection is done press Submit button to refresh the contents of the table in the right-side panel. Use Reset button to clean up the values in all the selector widgets.

  **Sites**

        Allows the user to select one or various sites.

Statistics
==========

  General statistics are provided in the left-side panel, statistics showed are a summary over all the sites where DIRAC can run pilots jobs.

  **Scheduled**

      Number of pilot jobs in status scheduled in all the sites.

  **Status**

      Summary status of all the sites.

  **Aborted_Hour**

      Number of pilot jobs aborted in all the sites in the last hour.

  **Waiting**

      Number of pilot jobs in status waiting in all the sites.

  **Submitted**

      Total number of pilot submitted last hour.

  **PilotsPerJob**

      Number of pilots required to run a user job.

  **Ready**

      Total number of pilots in status ready.

  **Running**

      Total number of pilots running over all the sites.

  **PilotJobEff(%)**

      Percentage of  pilots jobs finished whose status is done.

  **Done**

      Total number of pilot jobs whose status is done.

  **Aborted**

      Total number of pilot jobs aborted.

  **Done_Empty**

      Total number of pilot jobs in status done but **without output**.

  **Total**

      Total number of pilots.

Columns
=======

   The information on the selected sites is presented in the right-side panel in a form of a table.

  **Site**

      Site Name in DIRAC nomenclature.

  **CE**

      Site Computing Element name.

  **Status**

      General status of the site depending of pilot effectiveness.


          ==============      ===============================================================================
           **Status**          **Comment**
          ==============      ===============================================================================
              Bad              Site effectiveness less than 25% of pilot jobs executed successfully
              Poor             Site effectiveness less than 60% of pilot jobs executed successfully
              Fair             Site effectiveness less than 85% of pilot jobs executed successfully
              Good             Site effectiveness more than 85% of pilot jobs executed successfully
          ==============      ===============================================================================

  **PilotJobEff(%)**

      Percentage of pilots successful ran in the site.

  **PilotsPerJob**

      Number of pilot jobs required to execute an User Job.

  **Waiting**

      Number of pilot jobs waiting to be executed.

  **Scheduled**

      Number of pilot jobs scheduled in a particular site.

  **Running**

      Number of pilot jobs running in the site.

  **Done**

      Number of pilot jobs executed successfully in the site.

  **Aborted_Hour**

      Number of pilots aborted the last hour in the site.


Operations
==========

  Clicking on the line corresponding to a Site, one can obtain a menu which allows certain operations on Site Pilots Jobs. Currently, the following operations are available.


  **Show Pilots**

      Show in the right side panel all the Pilots Jobs related with the site.

  **Show Value**

      Show the value of the cell in a pop-up window.