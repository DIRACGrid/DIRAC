=========================
RAW Integrity
=========================

  This is part of DIRAC Web Portal project. For the description of the DIRAC Web Portal basic functionality look here.

    - `Description`_
    - `Selectors`_
    - `Global Sort`_
    - `Current Statistics`_
    - `Global Statistics`_
    - `Columns`_
    - `Operations`_


Description
==============

  The RAW Integrity provide information about files currently managed by the DIRAC Data Management System. It shows details of the selected files and allows certain file selection.


Selectors
===========

  Selector widgets are provided in the left-side panel. These are drop-down lists with values that can be selected. A single or several values can be chosen. Once the selection is done press Submit button to refresh the contents of the table in the right-side panel. Use Reset button to clean up the values in all the selector widgets.

  The following Selectors are available:

  **Status**

      Status of the file.

  **Storage Element**

      Name of Storage Element.

  **Time Start**

      **Time Start to look stored files**

  **Time End**

      **Time end to look stored files**

  **LFN**

      Logical file name.

Global Sort
============

  This selector allows the users sort the files using one of the options showed below:

    - Start Time
    - End Time
    - Status Ascending
    - Status Descending
    - Storage Ascending
    - Storage Descending
    - LFN

Current Statistics
==================

   Show status and numbers of selected files. The possible values of status are:

    ===========  =====================
      Status       Comment
    ===========  =====================
      Active
      Done
      Failed
    ===========  =====================


Global Statistics
==================

  Show status and numbers in a global way. The possible values of status are:

   ===========  =====================
     Status       Comment
   ===========  =====================
     Active
     Done
     Failed
   ===========  =====================


Columns
===========

  The information on the selected file is presented in the right-side panel in a form of a table. Note that not all the available columns are displayed by default. You can choose extra columns to display by choosing them in the menu activated by pressing on a menu button ( small triangle ) in any column title field.

  The following columns are provided:

  **LFN**

      Logical file name.

  **Status**

      Status of the file.

  **Site**

      Site name using DIRAC convention.

  **Storage Element**

      Storage Element name using DIRAC convention where the file is stored.

  **Checksum**

      **Value of the checksum file which is also calculated at the original write time at the Online storage. If the two checksums match the integrity of the file in CASTOR can be assumed.**

  **PFN**

      Physical File name.

  **Start Time (UTC)**

  **End Time (UTC)**

  **GUI**

Operations
============

  Clicking on the line corresponding to a file, one can obtain a menu which allows certain operations on the **Raw integrity**. Currently, the following operations are available:

  **Logging Info**

       Shows information about the file selected.

    - **Status**:
    - **Minor Status**:
    - **Start Time**: Start time
    - **Source**: File directory source.

  **Show Value**

      Show the value of the cell.
