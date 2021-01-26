.. _registry_vo:

Registry / VO - Subsections
==============================

In this section each Virtual Organization (VO) is described in a dedicated subsection.
The VO is a term coming from grid infrastructures where VO parameters are handled
by the VOMS services. In DIRAC VO is not necessarily corresponding to some VOMS
described VO. However, the VO options can include specific VOMS information. It is
not manadatory for the DIRAC VO to have the same name as the corresponding VOMS VO.
However, having these names the same can avoid confusions at the expense of having
names longer than necessary.


+----------------------------+------------------------------------------------------+--------------------------------------------------------------+
| **Name**                   | **Description**                                      | **Example**                                                  |
+----------------------------+------------------------------------------------------+--------------------------------------------------------------+
| *<VO_NAME>/VOAdmin*        | VO administrator user name                           | VOAdmin = joel                                               |
+----------------------------+------------------------------------------------------+--------------------------------------------------------------+
| *<VO_NAME>/VOAdminGroup*   | VO administrator group used for querying VOMS server | VOAdminGroup = lhcb_admin                                    |
|                            | If not specified, the VO "DefaultGroup" will be used |                                                              |
+----------------------------+------------------------------------------------------+--------------------------------------------------------------+
| *<VO_NAME>/VOMSName*       | VOMS VO name                                         | VOMSName = lhcb                                              |
+----------------------------+------------------------------------------------------+--------------------------------------------------------------+

VOMSServers subsection
------------------------

This subsection of the VO/<VO_NAME> section contains parameters of all the VOMS servers that can
be used with the given <VO_NAME>. It has a subsection per each VOMS server ( <VOMS_SERVER> ), the
name of the section is the host name of the VOMS server. These parameters are used in order
to create appropriate *vomses* and *vomsdir* directories when installing DIRAC clients.

+----------------------------+--------------------------------------------+-------------------------------------------------------------------+
| **Name**                   | **Description**                            | **Example**                                                       |
+----------------------------+--------------------------------------------+-------------------------------------------------------------------+
| *<VOMS_SERVER>/DN*         | DN of the VOMS server certificate          | DN = /O=GRID-FR/C=FR/O=CNRS/OU=CC-IN2P3/CN=cclcgvomsli01.in2p3.fr |
+----------------------------+--------------------------------------------+-------------------------------------------------------------------+
| *<VOMS_SERVER>/Port*       | The VOMS server port                       | Port = 15003                                                      |
+----------------------------+--------------------------------------------+-------------------------------------------------------------------+
| *<VOMS_SERVER>/CA*         | CA that issued the VOMS server certificate | CA = /C=FR/O=CNRS/CN=GRID2-FR                                     |
+----------------------------+--------------------------------------------+-------------------------------------------------------------------+
