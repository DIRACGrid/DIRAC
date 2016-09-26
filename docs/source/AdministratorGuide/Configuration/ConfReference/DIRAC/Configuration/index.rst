DIRAC / Configuration - Subsection
==================================

This subsection is used to configure the Configuration Servers attributes. It should not edited by hand since it is upated by the Master Configuration Server to reflect the current situation of the system.

+-------------------+----------------------------------------------------+----------------------------------------------------------------------+
| **Name**          | **Description**                                    | **Example**                                                          |
+-------------------+----------------------------------------------------+----------------------------------------------------------------------+
| *AutoPublish*     |                                                    | AutoPublish = yes                                                    |
+-------------------+----------------------------------------------------+----------------------------------------------------------------------+
| *EnableAutoMerge* | Allows Auto Merge. Takes a boolean value.          | EnableAutoMerge = yes                                                |
+-------------------+----------------------------------------------------+----------------------------------------------------------------------+
| *MasterServer*    | Define the primary master server.                  | MasterServer = dips://cclcgvmli09.in2p3.fr:9135/Configuration/Server |
+-------------------+----------------------------------------------------+----------------------------------------------------------------------+
| *Name*            | Name of Configuration file                         | Name = Dirac-Prod                                                    |
+-------------------+----------------------------------------------------+----------------------------------------------------------------------+
| *PropagationTime* |                                                    | PropagationTime = 100                                                |
+-------------------+----------------------------------------------------+----------------------------------------------------------------------+
| *RefreshTime*     | How many time the secondary servers are going to   | RefreshTime = 600                                                    |
|                   | refresh configuration from master.                 |                                                                      |
|                   | Expressed as Integer and seconds as unit.          |                                                                      |
+-------------------+----------------------------------------------------+----------------------------------------------------------------------+
| *SlavesGraceTime* |                                                    | SlavesGraceTime = 100                                                |
+-------------------+----------------------------------------------------+----------------------------------------------------------------------+
| *Servers*         | List of Configuration Servers installed. Expressed | Servers = dips://cclcgvmli09.in2p3.fr:9135/Configuration/Server      |
|                   | as URLs using dips as protocol.                    |                                                                      |
+-------------------+----------------------------------------------------+----------------------------------------------------------------------+
| *Version*         | CS configuration version used by DIRAC services    | Version = 2011-02-22 15:17:41.811223                                 |
|                   | as indicator when they need to reload the          |                                                                      |
|                   | configuration. Expressed using date format.        |                                                                      |
+-------------------+----------------------------------------------------+----------------------------------------------------------------------+


