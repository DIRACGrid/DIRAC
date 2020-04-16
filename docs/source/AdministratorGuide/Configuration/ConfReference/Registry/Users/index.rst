.. _registryUsers:

Registry / Users - Subsections
==============================

In this section each user is described using simple attributes. An subsection with the DIRAC user name must be created. Some of the attributes than can 
be included are mandatory and others are considered as helpers:

+----------------------------+-------------------------------------------------+--------------------------------------------------------------+
| **Name**                   | **Description**                                 | **Example**                                                  |
+----------------------------+-------------------------------------------------+--------------------------------------------------------------+
| *<DIRAC_USER_NAME>/DN*     | Distinguish name obtained from user certificate | DN = /O=GRID-FR/C=FR/O=CNRS/OU=CPPM/CN=Andrei Tsaregorodtsev |
|                            | (Mandatory)                                     |                                                              |
+----------------------------+-------------------------------------------------+--------------------------------------------------------------+
| *<DIRAC_USER_NAME>/Email*  | User e-mail  (Mandatory)                        | Email = atsareg@in2p3.fr                                     |
+----------------------------+-------------------------------------------------+--------------------------------------------------------------+
| *<DIRAC_USER_NAME>/mobile* | Cellular phone number                           | mobile = +030621555555                                       |
+----------------------------+-------------------------------------------------+--------------------------------------------------------------+
| *<DIRAC_USER_NAME>/Quota*  | Quota assigned to the user. Expressed in MBs.   | Quota = 300                                                  |
+----------------------------+-------------------------------------------------+--------------------------------------------------------------+

DNProperties - subsection
-------------------------

In `Registry / Users / <DIRAC_USER_NAME> / DNProperties` subsection describes the properties associated with each DN. It contains a sections with any name, that contains the DN name attribute and properties associated with that DN.

+-----------------------------------+-------------------------------------------------+--------------------------------------------------------------+
| **Name**                          | **Description**                                 | **Example**                                                  |
+-----------------------------------+-------------------------------------------------+--------------------------------------------------------------+
| *<DN_SUBSECTION>/DN*              | Distinguish name obtained from user certificate | DN = /O=GRID-FR/C=FR/O=CNRS/OU=CPPM/CN=Andrei Tsaregorodtsev |
|                                   | (Mandatory)                                     |                                                              |
+-----------------------------------+-------------------------------------------------+--------------------------------------------------------------+
| *<DN_SUBSECTION>/ProxyProviders*  | Proxy provider that can generate the proxy      | ProxyProviders = MY_DIRACCA                                  |
|                                   | certificate with DN in DN attribute.            |                                                              |
+-----------------------------------+-------------------------------------------------+--------------------------------------------------------------+
