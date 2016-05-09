Systems / Framework / <INSTANCE> / Service / BundleDelivery - Sub-subsection
============================================================================

Bundle delivery services is used to transfer Directories to clients by making tarballs.

+---------------------+---------------------------------------+---------------------------------------+
| **Name**            | **Description**                       | **Example**                           |
+---------------------+---------------------------------------+---------------------------------------+
| *CAs*               | Boolean, bundle CAs                   |  CAs = True                           |
+---------------------+---------------------------------------+---------------------------------------+
| *CRLs*              | Boolean, bundle CRLs                  |  CRLs = True                          |
+---------------------+---------------------------------------+---------------------------------------+
| *DirsToBundle*      | Section with Additional directories   | DirsToBundle/NameA = /opt/dirac/NameA |
|                     | to serve                              |                                       |
+---------------------+---------------------------------------+---------------------------------------+
