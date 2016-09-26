Systems / Configuration / <INSTANCE> / Agents /Bdii2CSAgent - Sub-subsection
============================================================================

Bdii2CSAgent is the agent in charge of updating sites parameters configuration for a specific VO.
- Queries BDII for Computing Elements (CEs) information and update the CS.
- Queries BDII for Storage Elements (SEs) information and update the CS.

The attributes of this agent are shown in the table below:

+-----------------------+-----------------------------------+-------------------------------------------------+
| **Name**              | **Description**                   | **Example**                                     |
+-----------------------+-----------------------------------+-------------------------------------------------+
| *AlternativeBDIIs*    | List of alternatives BDIIs        | AlternativeBDIIs = bdii01.in2p3.fr              |
+-----------------------+-----------------------------------+-------------------------------------------------+
| *ProcessCEs*          | Process Computing Elements        | ProcessCEs = True                               |
+-----------------------+-----------------------------------+-------------------------------------------------+
| *ProcessSEs*          | Process Storage Elements          | ProcessSEs = True                               |
+-----------------------+-----------------------------------+-------------------------------------------------+
| *MailTo*              | E-mail of the person in charge of | MailTo = hamar@cppm.in2p3.fr                    |
|                       | update the Sites configuration    |                                                 |
+-----------------------+-----------------------------------+-------------------------------------------------+
| *MailFrom*            | E-mail address used to send the   | MailFrom = dirac@mardirac.in2p3.fr              |
|                       | information to be updated         |                                                 |
+-----------------------+-----------------------------------+-------------------------------------------------+
| *VirtualOrganization* | Name of the VO                    | VirtualOrganization = vo.formation.idgrilles.fr |
+-----------------------+-----------------------------------+-------------------------------------------------+
