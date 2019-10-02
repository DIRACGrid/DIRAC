.. _dirac-registry-cs:

Registry / Groups - Subsections
===============================

This subsection is used to describe DIRAC groups registered in the server.

+----------------------------------+------------------------------------------------+-----------------------------+
| **Name**                         | **Description**                                | **Example**                 |
+----------------------------------+------------------------------------------------+-----------------------------+
| *<GROUP_NAME>*                   | Subsection, represents the name of the group   | dirac_user                  |
+----------------------------------+------------------------------------------------+-----------------------------+
| *<GROUP_NAME>/Users*             | DIRAC users logins than belongs to the group   | Users = atsareg             |
|                                  |                                                | Users += msapunov           |
+----------------------------------+------------------------------------------------+-----------------------------+
| *<GROUP_NAME>/Properties*        | Properties of the group, this will change      | Properties = NormalUser     |
|                                  | the permissions of the group.                  |                             |
+----------------------------------+------------------------------------------------+-----------------------------+
| *<GROUP_NAME>/DownloadableProxy* | Permission to download proxy with this group,  | DownloadableProxy = False   |
|                                  | by default it's True(downloadable)             |                             |
+----------------------------------+------------------------------------------------+-----------------------------+
| *<GROUP_NAME>/VOMSRole*          | Role of the users in the VO                    | VOMSRole = /biomed          |
+----------------------------------+------------------------------------------------+-----------------------------+
| *<GROUP_NAME>/VOMSVO*            | Virtual organization associated with the group | VOMSVO = biomed             |
+----------------------------------+------------------------------------------------+-----------------------------+
| *JobShare*                       | Just for normal users                          | JobShare = 200              |
+----------------------------------+------------------------------------------------+-----------------------------+
| *AutoUploadProxy*                | Controls automatic Proxy upload by             | AutoUploadProxy = True      |
|                                  | dirac-proxy-init                               |                             |
+----------------------------------+------------------------------------------------+-----------------------------+
| *AutoUploadPilotProxy*           | Controls automatic Proxy upload by             | AutoUploadPilotProxy = True |
|                                  | dirac-proxy-init for Pilot groups              |                             |
+----------------------------------+------------------------------------------------+-----------------------------+
| *AutoAddVOMS*                    | Controls automatic addition of VOMS            | AutoAddVOMS = True          |
|                                  | extension by dirac-proxy-init                  |                             |
+----------------------------------+------------------------------------------------+-----------------------------+


* Default properties by group:

  ** dirac_admin:

   -   Properties = AlarmsManagement
   -   Properties += ServiceAdministrator
   -   Properties += CSAdministrator
   -   Properties += JobAdministrator
   -   Properties += FullDelegation
   -   Properties += ProxyManagement
   -   Properties += Operator

  ** dirac_pilot

   -  Properties = GenericPilot
   -  Properties += LimitedDelegation
   -  Properties += Pilot

  ** dirac_user

   - Properties = NormalUser
