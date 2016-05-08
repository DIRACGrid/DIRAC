Registry / VO - Subsections
==============================

In this section each Virtual Organization (VO) is described in a dedicated subsection.
The VO is a term coming from grid infrastructures where VO parameters are handled
by the VOMS services. In DIRAC VO is not necessarily corresponding to some VOMS
described VO. However, the VO options can include specific VOMS information. It is
not manadatory for the DIRAC VO to have the same name as the corresponding VOMS VO.
However, having these names the same can avoid confusions at the expense of having
names longer than necessary.


+----------------------------+-------------------------------------------------+--------------------------------------------------------------+
| **Name**                   | **Description**                                 | **Example**                                                  |
+----------------------------+-------------------------------------------------+--------------------------------------------------------------+
| *<VO_NAME>/VOAdmin*        | VO administrator user name                      | VOAdmin = joel                                               |
+----------------------------+-------------------------------------------------+--------------------------------------------------------------+
| *<VO_NAME>/VOMSName*       | VOMS VO name                                    | VOMSName = lhcb                                              |
+----------------------------+-------------------------------------------------+--------------------------------------------------------------+
| *<VO_NAME>/SubmitPools*    | Default Submit Pools for the users belonging    | SubmitPools = lhcbPool                                       |
|                            | to the VO                                       |                                                              |
+----------------------------+-------------------------------------------------+--------------------------------------------------------------+

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

VOMSServices subsection
------------------------

This subsection contains URLs to obtain specific VOMS informations.

+----------------------+------------------------------------+------------------------------------------------------------------------------------+
| **Name**             | **Description**                    | **Example**                                                                        |
+----------------------+------------------------------------+------------------------------------------------------------------------------------+
| *VOMSAttributes*     | URL to get VOMS attributes         | VOMSAttributes = https://voms2.cern.ch:8443/voms/lhcb/services/VOMSAttributes      |
+----------------------+------------------------------------+------------------------------------------------------------------------------------+
| *VOMSAdmin*          | URL to get VOMS administrator info | VOMSAdmin = https://voms2.cern.ch:8443/voms/lhcb/services/VOMSAdmin                |
+----------------------+------------------------------------+------------------------------------------------------------------------------------+
| *VOMSCompatibility*  | URL to get VOMS compatibility info | VOMSCompatibility = https://voms2.cern.ch:8443/voms/lhcb/services/VOMSCompatibility|
+----------------------+------------------------------------+------------------------------------------------------------------------------------+
| *VOMSCertificates*   | URL to get VOMS certificate info   | VOMSCertificates = https://voms2.cern.ch:8443/voms/lhcb/services/VOMSCertificates  |
+----------------------+------------------------------------+------------------------------------------------------------------------------------+
