Resources / Computing
=====================

In this section options for ComputingElements can be set


Resources / Computing / CEDefaults
==================================



+---------------------------------+------------------------------------------------+-----------------------------------+
| **Name**                        | **Description**                                | **Example**                       |
+---------------------------------+------------------------------------------------+-----------------------------------+
| GridEnv                         |Default environment file sourced before calling | /opt/dirac/gridenv                |
|                                 |grid commands, without extension                |                                   |
+---------------------------------+------------------------------------------------+-----------------------------------+
| XRSLExtraString                 | Default additional string for ARC submit files |                                   |
+---------------------------------+------------------------------------------------+-----------------------------------+

	     

Resources / Computing / CEDefaults / HTCondorCE
===============================================

Options for the HTCondorCEs


+---------------------+----------------------------------------------------+-----------------------------------------------------------+
| **Name**            | **Description**                                    | **Example**                                               |
+---------------------+----------------------------------------------------+-----------------------------------------------------------+
| ExtraSubmitstring   | Default additional string for the condor submit    | request_cpus = 8 \n periodic_remove = ...                 |
|                     | file. Separate entries with "\n".                  |                                                           |
+---------------------+----------------------------------------------------+-----------------------------------------------------------+
| WorkingDirectory    | Directory where the pilot log files are stored     | /opt/dirac/pro/runit/WorkloadManagement/SiteDirectorHT    |
|                     | locallly. Also temproray files like condor submit  |                                                           |
|                     | files are kept here.                               |                                                           |
+---------------------+----------------------------------------------------+-----------------------------------------------------------+
| DaysToKeepLogFiles  | How many days pilot log files are kept on the disk | 15                                                        |
|                     | before they are removed                            |                                                           |
+---------------------+----------------------------------------------------+-----------------------------------------------------------+

