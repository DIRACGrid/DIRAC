Resources / Computing
=====================

In this section options for ComputingElements can be set


Location for Parameters
-----------------------

Options for computing elements can be set at different levels, from lowest to
highest prority

  /Resources/Computing/CEDefaults
	For all computing elements
  /Resources/Computing/<CEType>
	 For CEs of a given type, e.g., HTCondorCE or ARC
  /Resources/Sites/<grid>/<site>/CEs
	 For all CEs at a given site
  /Resources/Sites/<grid>/<site>/CEs/<CEName>
	 For the specific CE

Values are overwritten.


General Parameters
------------------

These parameters are valid for all types of computing elements

+---------------------------------+------------------------------------------------+-----------------------------------+
| **Name**                        | **Description**                                | **Example**                       |
+---------------------------------+------------------------------------------------+-----------------------------------+
| GridEnv                         |Default environment file sourced before calling | /opt/dirac/gridenv                |
|                                 |grid commands, without extension '.sh'.         | (when the file is gridenv.sh)     |
+---------------------------------+------------------------------------------------+-----------------------------------+




ARC CE Parameters
-----------------

+---------------------------------+---------------------------------------------------+-------------------------------------------------------------+
| **Name**                        | **Description**                                   | **Example**                                                 |
+---------------------------------+---------------------------------------------------+-------------------------------------------------------------+
| XRSLExtraString                 |  Default additional string for ARC submit files   |                                                             |
+---------------------------------+---------------------------------------------------+-------------------------------------------------------------+
| Host                            | The host for the ARC CE, used to overwrite the    |                                                             |
|                                 | ce name                                           |                                                             |
+---------------------------------+---------------------------------------------------+-------------------------------------------------------------+
| WorkingDirectory                | Directory where the pilot log files are stored    |   /opt/dirac/pro/runit/WorkloadManagement/SiteDirectorArc   |
|                                 | locally.                                          |                                                             |
+---------------------------------+---------------------------------------------------+-------------------------------------------------------------+


.. _res-comp-htcondor:

HTCondorCE Parameters
---------------------

Options for the HTCondorCEs

+---------------------+----------------------------------------------------+-----------------------------------------------------------+
| **Name**            | **Description**                                    | **Example**                                               |
+---------------------+----------------------------------------------------+-----------------------------------------------------------+
| ExtraSubmitString   | Additional string for the condor submit            | request_cpus = 8 \\n periodic_remove = ...                |
|                     | file. Separate entries with "\\n".                 |                                                           |
+---------------------+----------------------------------------------------+-----------------------------------------------------------+
| WorkingDirectory    | Directory where the pilot log files are stored     | /opt/dirac/pro/runit/WorkloadManagement/SiteDirectorHT    |
|                     | locally. Also temproray files like condor submit   |                                                           |
|                     | files are kept here. This option is only read from |                                                           |
|                     | the global CEDefaults/HTCondorCE location.         |                                                           |
+---------------------+----------------------------------------------------+-----------------------------------------------------------+
| DaysToKeepLogFiles  | How many days pilot log files are kept on the disk | 15                                                        |
|                     | before they are removed                            |                                                           |
+---------------------+----------------------------------------------------+-----------------------------------------------------------+
