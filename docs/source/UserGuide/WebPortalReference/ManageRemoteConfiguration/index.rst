======================================
Manage Remote Configuration
======================================

  This is part of DIRAC Web Portal project. For the description of the DIRAC Web Portal basic functionality look here.


    - `Description`_
    - `Text Actions`_
    - `Modification Actions`_
    - `Operations`_



Description
============

  Show Remote Configuration allows administrators navigate in a friendly way through the configuration file, the configuration of the servers is managed by DIRAC Configuration System.

Text Actions
============

  Text actions are provided in the left-side panel, in this moment just two options are available:

  **View configuration as text**

      This action shows the configuration file in text format in a pop-up window.

  **Download configuration**

      This action permit download the configuration file to local machine.

Modification Actions
=====================

  Modification actions are provided in the left-side panel, the available modifications are:

  **Re-download configuration data from server**

      Allows DIRAC administrators to update or download again, depending of the case, the configuration used the server into the web browser.

  **Show differences with server**

      This option shows the differences between file loaded into web browser and the file used currently by the server.

  **Commit configuration**

       Allow DIRAC Administrator to commit a new configuration file into the server.

Operations
==========

  In the right side panel the configuration file is exposed using a **schema or folder metaphor**, this metaphor allows DIRAC Administrators to expand or collapse each folder and sub folders in order to look at, add, remove or change the attributes and respective values into the configuration file. After any modification of the configuration file is mandatory to commit the configuration file, executing this action the new configuration file is copied to the server, the service is restarted and loaded into the system.
