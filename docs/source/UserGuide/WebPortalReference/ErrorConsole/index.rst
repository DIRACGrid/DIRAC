
=========================
Error Console 
=========================

  This is part of DIRAC Web Portal project. For the description of the DIRAC Web Portal basic functionality look here.

    - `Description`_
    - `Selectors`_
    - `Columns`_


Description
===========

  Error Console provide information about Errors reported by DIRAC services and managed by Framework System Logging Report. Details of found errors are showed, also this information can be refined using the available selectors in the left side panel.


Selectors
=========

  Selector widgets are provided in the left-side panel.  A single or several values can be chosen. Once the selection is done press Submit button to refresh the contents of the table in the right-side panel. Use Reset button to clean up the values in all the selector widgets. Available selectors in this case are:

  **Start Date**

      Date to start Logs selection

  **Final Date**

      Date until logs must be showed.

Columns
=======

  The information on selected logs is presented in the right-side panel in a form of a table. Note that not all the available columns are displayed by default. You can choose extra columns to display by choosing them in the menu activated by pressing on a menu button ( small triangle ) in any column title field.

  The following columns are provided:

  **Components**

      DIRAC Component related with the error.

  **SubSystem**

      **UNKNOWN??**

  **Error**

       Brief error description.

  **LogLevel**

       Log Level associated with the fault, this help to determinate the importance of the error

             +---------------+------------------------------------------------------------------------------+
             |**Log Level**  |    **Description**                                                           |
             +---------------+------------------------------------------------------------------------------+
             |DEBUG          |The DEBUG Level is a fine-grained event used to debug the service or agent    |
             +---------------+------------------------------------------------------------------------------+
             |INFO           |The INFO Level is a coarse-grained event used to show application process     |
             +---------------+------------------------------------------------------------------------------+
             |WARN           |The WARN Level show warns about future possible errors in the service or agent|
             +---------------+------------------------------------------------------------------------------+
             |ERROR          |The ERROR Level show errors occurred, the services or agents can still run    |
             +---------------+------------------------------------------------------------------------------+
             |FATAL          |The FATAL Level show errors than makes service or agent stop                  |
             +---------------+------------------------------------------------------------------------------+


  **SiteName**

      Site names associated with the error.

  **Example**

      Shows one error log entry.

  **OwnerDN**

      Distinguish name of the entity associated with the error.

  **OwnerGroup**

      DIRAC group associated with the error.

  **IP**

      Server IP Address associated with the error.

  **Message Time**

      UTC time stamp in the log file when the error was reported.

  **Number of errors**

      Number of error occurrences.
