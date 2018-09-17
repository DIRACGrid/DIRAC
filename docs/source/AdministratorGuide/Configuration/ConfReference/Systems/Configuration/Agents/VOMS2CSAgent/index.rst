Systems / Configuration / <INSTANCE> / Agents /VOMS2CSAgent - Sub-subsection
================================================================================

VOMS2CSAgent queries VOMS servers and updates the users and groups as defined in the Configuration Registry
for the given VO and groups in this VO. It performs the following operations:

* Extracts user info from the VOMS server using its REST interface
* Finds user DN's not yet registered in the DIRAC Registry
* For each new DN it constructs a DIRAC login name by a best guess or using the nickname VOMS attribute
* Registers new users to the DIRAC Registry including group membership
* Updates information for already registered users
* Sends report for performed operation to the VO administrator

The agent is performing its operations with credentials of the VO administrator as defined
in the /Registry/VO/<VO_name> configuration section.

The configuration options of this agent are shown in the table below:

+-----------------------+--------------------------------------+----------------------------------------------+
| **Name**              | **Description**                      | **Example**                                  |
+-----------------------+--------------------------------------+----------------------------------------------+
| *VO*                  | List of VO names                     | VO = biomed, eiscat.se, compchem             |
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
| *AutoDeleteUsers*     | Users no more registered in VOMS are |                                              |
|                       | automatically deleted from DIRAC     | AutoDeleteUsers = False                      |
+-----------------------+--------------------------------------+----------------------------------------------+
| *DetailedReport*      | Detailed report on users per group   |                                              |
|                       | sent to the VO administrator         | DetailedReport = True                        |
+-----------------------+--------------------------------------+----------------------------------------------+
| *MakeHomeDirectory*   | Automatically create user home       |                                              |
|                       | directory in the File Catalog        | MakeHomeDirectory = False                    |
+-----------------------+--------------------------------------+----------------------------------------------+

Remark: options *AutoAddUsers*, *AutoModifyUsers*, *AutoDeleteUsers* can be overridden by the corresponding
options defined in the /Registry/VO/<VO_name> configuration section.