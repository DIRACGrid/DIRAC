====================================
Proxy Action Logs
====================================

  This is part of DIRAC Web Portal project. For the description of the DIRAC Web Portal basic functionality look here.


    - `Description`_
    - `Columns`_
    - `Filters`_


Description
===========

  Proxy Action Logs page present on a table each operation related with proxies, related with users, hosts or services, into DIRAC system.

Columns
===========

  **Timestamp (UTC)**

      Time stamp (UTC) when the operation was executed.

  **Action**

      Describe the action executed using the proxy, by example: store proxy, download voms proxy, set persistent proxy.

  **IssuerDN**

        Certificate Distinguish Name of the entity who request perform the operation.

  **IssuerGroup**

         DIRAC group associated with IssuerDN who is requesting the operation.

  **TargetDN**

          Distinguish Name of Certificate entity who request to perform the operation.

  **TargetGroup**

         DIRAC group associated whit the TargetDN over who the operation is performed.


Filters
========

  Filters allows the user to refine logs selection according one or more attributes. Filters are available as a combination of a menu than appears clicking into a log row and options available in the bottom field, filters available are described below:

  The menu show options are:

  **Filter by action**

      Depending of the value of the log than was clicked will be created the filter.

  **Filter by issuer DN**

      Depending of the value of the log than was clicked will be created the filter.

  **Filter by target DN**

      Depending of the value of the log than was clicked will be created the filter.

  **Filter by target group**

      Depending of the value of the log than was clicked will be created the filter.

  At the bottom field appears the following items:

  **Page Manager**

      Allow the user navigate through all the log pages.

  **Refresh button**

      This button user to refresh the page in fly time and apply the filter to the logs.

  **Items displaying per page**

      Deploy a menu than present option of 25, 50, 100, 150 actions by page

  **After**

      Show the logs actions performed after the date selected.

  **Before**

      Show the logs actions performed before the date selected.

  **Filters**

      Show selected filters to perform the action.

  **Clear Filters**

      This button clear the filters used in the previous selection.

  **NOTE:** To perform any filtering action must be pressed the refresh button in the bottom field.
