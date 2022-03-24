Resources / StorageElements and StorageElementBases- Subsections
==================================================================

All the storages elements available for the users are described in these subsections. Base Storage Elements, corresponding to abstract Storage Element, must be defined in the Resources/StorageElementBases section while other Storage Elements, like inherited and simple Storage Elements, must be configured in the Resources/StorageElement section. This information will be moved below the Sites section.

+---------------------------------------------+--------------------------------------------------+-----------------------------+
| **Name**                                    | **Description**                                  | **Example**                 |
+---------------------------------------------+--------------------------------------------------+-----------------------------+
| *DefaultProtocols*                          | Default protocols than can be used to interact   | DefaultProtocols = rfio     |
|                                             | with the storage elements.                       | DefaultProtocols += file    |
|                                             |                                                  | DefaultProtocols += root    |
|                                             |                                                  | DefaultProtocols += gsiftp  |
+---------------------------------------------+--------------------------------------------------+-----------------------------+
| *SITE-disk*                                 | Subsection. DIRAC name for the storage element   | CPPM-disk                   |
+---------------------------------------------+--------------------------------------------------+-----------------------------+
| *SITE-disk/BackendType*                     | Type of storage element. Possible values are:    | BackendType = dpm           |
|                                             | dmp, DISET, dCache, Storm                        |                             |
+---------------------------------------------+--------------------------------------------------+-----------------------------+
| *SITE-disk/ReadAccess*                      | Allow read access                                | ReadAccess = Active         |
|                                             | Possible values are: Active, InActive            |                             |
+---------------------------------------------+--------------------------------------------------+-----------------------------+
| *SITE-disk/WriteAccess*                     | Allow write access                               | WriteAccess = Active        |
|                                             | Possible values are: Active, InActive            |                             |
+---------------------------------------------+--------------------------------------------------+-----------------------------+
| *SITE-disk/RemoveAccess*                    | Allow removal of files at this SE                | RemoveAccess = Active       |
|                                             | Possible values are: Active, InActive            |                             |
+---------------------------------------------+--------------------------------------------------+-----------------------------+
| *SITE-disk/SEType*                          | Type of SE                                       | SEType = T0D1               |
|                                             | Possible values are: T0D1, T1D0, D1T0            |                             |
+---------------------------------------------+--------------------------------------------------+-----------------------------+
| *SITE-disk/AccessProtocol.<#>*              | Subsection. Access protocol number               | AccessProtocol.1            |
+---------------------------------------------+--------------------------------------------------+-----------------------------+
| *SITE-disk/AccessProtocol.<#>/Access*       | Access type to the resource                      | Access = Remote             |
+---------------------------------------------+--------------------------------------------------+-----------------------------+
| *SITE-disk/AccessProtocol.<#>/Host*         | Storage element fully qualified hostname         | Host = se01.in2p3.fr        |
+---------------------------------------------+--------------------------------------------------+-----------------------------+
| *SITE-disk/AccessProtocol.<#>/Path*         | Path in the SE just before the VO directory      | Path = /dpm/in2p3.fr/home   |
+---------------------------------------------+--------------------------------------------------+-----------------------------+
| *SITE-disk/AccessProtocol.<#>/Port*         | Port number to access the data                   | Port = 8446                 |
+---------------------------------------------+--------------------------------------------------+-----------------------------+
| *SITE-disk/AccessProtocol.<#>/Protocol*     | Protocol to be used to interact with the SE      | Protocol = srm              |
+---------------------------------------------+--------------------------------------------------+-----------------------------+
| *SITE-disk/AccessProtocol.<#>/PluginName*   | Protocol name to be used to interact with the SE | PluginName = GFAL2_SRM2     |
+---------------------------------------------+--------------------------------------------------+-----------------------------+
| *SITE-disk/AccessProtocol.<#>/WSUrl*        | URL from WebServices                             | WSUrl = /srm/managerv2?SFN= |
+---------------------------------------------+--------------------------------------------------+-----------------------------+
