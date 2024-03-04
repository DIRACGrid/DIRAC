Systems / WorkloadManagement / <INSTANCE> / Agents / PilotStatusAgent - Sub-subsection
======================================================================================

The Pilot Status Agent updates the status of the pilot jobs if the PilotAgents database.

Special attributes for this agent are:

+--------------------------+--------------------------------------------+-----------------------------------+
| **Name**                 | **Description**                            | **Example**                       |
+--------------------------+--------------------------------------------+-----------------------------------+
| *PilotAccountingEnabled* | Boolean type attribute than allows to      | PilotAccountingEnabled = Yes      |
|                          | specify if accounting is enabled           |                                   |
+--------------------------+--------------------------------------------+-----------------------------------+
| *PilotStalledDays*       | Number of days without response of a pilot | PilotStalledDays = 3              |
|                          | before be declared as Stalled              |                                   |
+--------------------------+--------------------------------------------+-----------------------------------+
