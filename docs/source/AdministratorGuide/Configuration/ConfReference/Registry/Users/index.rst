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
| *<DIRAC_USER_NAME>/CN*     | Canonical name of certification authority who   | CN = /C=FR/O=CNRS/CN=GRID2-FR                                |
|                            | sign the certificate.                           |                                                              |
+----------------------------+-------------------------------------------------+--------------------------------------------------------------+
| *<DIRAC_USER_NAME>/Email*  | User e-mail  (Mandatory)                        | Email = atsareg@in2p3.fr                                     |
+----------------------------+-------------------------------------------------+--------------------------------------------------------------+
| *<DIRAC_USER_NAME>/mobile* | Cellular phone number                           | mobile = +030621555555                                       |
+----------------------------+-------------------------------------------------+--------------------------------------------------------------+
| *<DIRAC_USER_NAME>/Quota*  | Quota assigned to the user. Expressed in MBs.   | Quota = 300                                                  |
+----------------------------+-------------------------------------------------+--------------------------------------------------------------+
