Systems / Configuration / <INSTANCE> / Agents /VOMS2CSAgent - Sub-subsection
================================================================================

Queries VOMRS servers and updates the users and groups as defined in the Registry/VOMS/Mapping section.

The attributes of this agent are showed in the table below:

+-----------------------+--------------------------------------+----------------------------------------------+
| **Name**              | **Description**                      | **Example**                                  |
+-----------------------+--------------------------------------+----------------------------------------------+
| *MailTo*              | E-mail of the person in charge of    | MailTo = hamar@cppm.in2p3.fr                 |
|                       | update the Sites configuration       |                                              |
+-----------------------+--------------------------------------+----------------------------------------------+
| *MailFrom*            | E-mail address used to send the      | MailFrom = dirac@mardirac.in2p3.fr           |
|                       | information to be updated            |                                              |
+-----------------------+--------------------------------------+----------------------------------------------+
| *AutoAddUsers*        | If users will be added automatically | AutoAddUsers = True                          |
+-----------------------+--------------------------------------+----------------------------------------------+
| *AutoModifyUsers*     | If users will be modified            |                                              |
|                       | automatically                        | AutoModifyUsers = True                       |
+-----------------------+--------------------------------------+----------------------------------------------+
