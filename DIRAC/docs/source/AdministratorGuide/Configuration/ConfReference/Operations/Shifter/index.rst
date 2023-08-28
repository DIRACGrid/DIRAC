Operations / Shifter - Subsection
=================================

In this subsection administrators may specify a list of user/group pairs whom
proxy certificates will be used for executing actions outside of the DIRAC environment.

Examples include, but are not limited to::
   - issuing transfer requests to an external system (e.g. FTS3)
   - querying grid databases (e.g. GOC DB)



+--------------------------+--------------------------+----------------------+
| **Name**                 | **Description**          | **Example**          |
+--------------------------+--------------------------+----------------------+
| *<ShifterName>*          | Name of service managers | Admin                |
|                          |                          | ProductionManager    |
|                          |                          | DataManager          |
|                          |                          | MonteCarloGeneration |
|                          |                          | DataProcessing       |
+--------------------------+--------------------------+----------------------+
| *<ShifterName>/User*     | DIRAC user name          | User = vhamar        |
+--------------------------+--------------------------+----------------------+
| *<ShifterName/Group*     | DIRAC user group         | Group = dirac_admin  |
+--------------------------+--------------------------+----------------------+

Running agents can use these "shifters" for executing the examples above:
agents requiring to act with a credential can specify the option **shifterProxy**,
or using a certain default, like "DataManager".

In general, to force any Agent to execute using a "shifter" credential,
instead of the certificate of the server it is only necessary to add a valid **shifterProxy**
option in its configuration (in the /Systems section).
