Systems / Transformation / <INSTANCE> / Services - Sub-subsection
=================================================================

All the services have common options to be configured for each one. Those options are
presented in the following table:

+-------------------------+----------------------------------------------+-----------------------------+
| **Name**                | **Description**                              | **Example**                 |
+-------------------------+----------------------------------------------+-----------------------------+
| *LogLevel*              | Level of log verbosity                       | LogLevel = INFO             |
+-------------------------+----------------------------------------------+-----------------------------+
| *LogBackends*           | Log backends                                 | LogBackends = stdout        |
|                         |                                              | LogBackends += ...          |
+-------------------------+----------------------------------------------+-----------------------------+
| *MaskRequestParameters* | Request to mask the values, possible values: | MaskRequestParameters = yes |
|                         | yes or no                                    |                             |
+-------------------------+----------------------------------------------+-----------------------------+
| *MaxThreads*            | Maximum number of threads used in parallel   | MaxThreads = 50             |
|                         | for the server                               |                             |
+-------------------------+----------------------------------------------+-----------------------------+
| *Port*                  | Port useb by DIRAC service                   | Port = 9140                 |
+-------------------------+----------------------------------------------+-----------------------------+
| *Protocol*              | Protocol used to comunicate with the service | Protocol = dips             |
+-------------------------+----------------------------------------------+-----------------------------+
| *Authorization*         | Subsection used to define which kind of      | Authorization               |
|                         | Authorization is required to talk with the   |                             |
|                         | service                                      |                             |
+-------------------------+----------------------------------------------+-----------------------------+
| *Authorization/Default* | Define to who is required the authorization  | Default = all               |
+-------------------------+----------------------------------------------+-----------------------------+

Transformation services are:

.. toctree::
   :maxdepth: 2
   
   TransformationManager/index
