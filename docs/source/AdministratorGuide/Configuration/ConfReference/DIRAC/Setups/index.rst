DIRAC / Setups - Subsection
===========================

In this subsection all the installed Setups are defined.

+----------------------+-------------------------------------------------+-----------------------------------+
| **Name**             | **Description**                                 | **Example**                       |
+----------------------+-------------------------------------------------+-----------------------------------+
| *Accounting*         | Describe the instance to be used for this setup | Accounting = Production           |
+----------------------+-------------------------------------------------+-----------------------------------+
| *Configuration*      | Describe the instance to be used for this setup | Configuration = Production        |
+----------------------+-------------------------------------------------+-----------------------------------+
| *DataManagement*     | Describe the instance to be used for this setup | DataManagement = Production       |
+----------------------+-------------------------------------------------+-----------------------------------+
| *Framework*          | Describe the instance to be used for this setup | Framework = Production            |
+----------------------+-------------------------------------------------+-----------------------------------+
| *RequestManagement*  | Describe the instance to be used for this setup | RequestManagement = Production    |
+----------------------+-------------------------------------------------+-----------------------------------+
| *StorageManagement*  | Describe the instance to be used for this setup | StorageManagement = Production    |
+----------------------+-------------------------------------------------+-----------------------------------+
| *WorkloadManagement* | Describe the instance to be used for this setup | WorkloadManagement = Production   |
+----------------------+-------------------------------------------------+-----------------------------------+


For each Setup known to the installation, there must be a subsection with the appropriated name.  Each option represents a DIRAC System available in the Setup and the Value is the instance of System that is used in that setup. For instance, since the Configuration is unique for the whole installation, all setups should have the same instance for the Configuration systems. 