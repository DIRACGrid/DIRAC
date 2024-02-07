Systems / WorkloadManagement / <INSTANCE> / Service / SandboxStore - Sub-subsection
===================================================================================

SandboxHandler is the implementation of the Sandbox service in the DISET framework

Some extra options are required to configure this service:

+---------------------------+----------------------------------------------+-----------------------------------------+
| **Name**                  | **Description**                              | **Example**                             |
+---------------------------+----------------------------------------------+-----------------------------------------+
| *BasePath*                | Base path where the files are stored         | BasePath = /opt/dirac/storage/sandboxes |
|                           | task queues in the system                    |                                         |
+---------------------------+----------------------------------------------+-----------------------------------------+
| *MaxSandboxSize*          | Maximum size of sanbox files expressed in MB | MaxSandboxSize = 10                     |
+---------------------------+----------------------------------------------+-----------------------------------------+
