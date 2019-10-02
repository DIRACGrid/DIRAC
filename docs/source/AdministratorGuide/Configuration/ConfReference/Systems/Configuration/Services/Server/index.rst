.. _ConfigurationServer:

Systems / Configuration / <INSTANCE> / Service / Server - Sub-subsection
========================================================================

In this subsection the Server service is configured. The attributes are showed in the following table:

+------------------------------------+--------------------------------------------+-------------------------------------------------------------------------+
| **Name**                           | **Description**                            | **Example**                                                             |
+------------------------------------+--------------------------------------------+-------------------------------------------------------------------------+
| *HandlerPath*                      | Relative path directory where the          | HandlerPath = DIRAC/ConfigurationSystem/Service/ConfigurationHandler.py |
|                                    | service is located                         |                                                                         |
+------------------------------------+--------------------------------------------+-------------------------------------------------------------------------+
| *Port*                             | Port where the service is responding       | Port = 9135                                                             |
+------------------------------------+--------------------------------------------+-------------------------------------------------------------------------+
| *Authorization*                    | Subsection to configure authorization over | Authorization                                                           |
|                                    | the service                                |                                                                         |
+------------------------------------+--------------------------------------------+-------------------------------------------------------------------------+
| *Authorization/Default*            | Default authorization                      | Default = all                                                           |
+------------------------------------+--------------------------------------------+-------------------------------------------------------------------------+
| *Authorization/commitNewData*      | Define who can commit new configuration    | commitNewData = CSAdministrator                                         |
+------------------------------------+--------------------------------------------+-------------------------------------------------------------------------+
| *Authorization/getVersionContents* | Define who can get version contents        | getVersionContents = CSAdministrator                                    |
+------------------------------------+--------------------------------------------+-------------------------------------------------------------------------+
| *Authorization/rollBackToVersion*  | Define who can roll back the configuration | rollBackToVersion = ServiceAdministrator                                |
|                                    | to a previous version                      | rollBackToVersion += CSAdministrator                                    |
+------------------------------------+--------------------------------------------+-------------------------------------------------------------------------+
