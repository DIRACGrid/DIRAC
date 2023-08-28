=============================
Sites Summary
=============================

  This is part of DIRAC Web Portal project. For the description of the DIRAC Web Portal basic functionality look here.

    - `Description`_
    - `Selectors`_
    - `Columns`_
    - `Operations`_

Description
===========

  Site Summary provide information about Sites managed by the DIRAC ***Workload Management System***. It shows details of the selected Sites and allows certain selections.


Selectors
=========

  Selector widgets are provided in the left-side panel. These are drop-down lists with values that can be selected. A single or several values can be chosen. Once the selection is done press Submit button to refresh the contents of the table in the right-side panel. Use Reset button to clean up the values in all the selector widgets.

  **Status**

  **GridType**

  **MaskStatus**

  **Country**

Columns
=======

  **Tier**

    Show the Tier associated with the site.

  **GridType**

    Grid type of the site, by example: DIRAC, gLite.

  **Country**

    Country where the site is located.

  **MaskStatus**

    Mask status of the site, it can take two values: **Allowed or Banned**

  **Efficiency (%)**

    Site percentage of efficiency, the values associated are:

  **Status**

    =========  ======================================================================
      Status     Comment
    =========  ======================================================================
      Bad       Site effectiveness less than 25% of pilot jobs executed successfully
      Poor      Site effectiveness less than 60% of pilot jobs executed successfully
      Fair      Site effectiveness less than 85% of pilot jobs executed successfully
      Good      Site effectiveness more than 85% of pilot jobs executed successfully
    =========  ======================================================================

  **Received**

      Number of Pilots Jobs such status is Received in the site.

  **Checking**

      Number of Pilots Jobs such status is Checking in the site.

  **Staging**

      Number of Pilots Jobs such status is Staging in the site.

  **Waiting**

      Number of Pilots Jobs such status is Waiting in the site.

  **Matched**

      Number of Pilots Jobs such status is Matched in the site.

  **Running**

      Number of Pilots Jobs such status is Running in the site.

  **Completed**

      Number of Pilots Jobs such status is Completed in the site.

  **Done**

      Number of Pilots Jobs such status is Done in the site.

  **Stalled**

      Number of Pilots Jobs such status is Stalled in the site.

  **Failed**

      Number of Pilots Jobs such status is Failed in the site.

Operations
==========
