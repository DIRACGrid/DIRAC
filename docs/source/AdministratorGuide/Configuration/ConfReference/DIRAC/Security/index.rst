DIRAC / Security - Subsection
=============================

In this subsection security server configuration attributes are defined.

+------------------------+------------------------------------------------+------------------------------------------------------+
| **Name**               | **Description**                                | **Example**                                          |
+------------------------+------------------------------------------------+------------------------------------------------------+
| *CertFile*             | Directory where host certificate is located in | CertFile = /opt/dirac/etc/grid-security/hostcert.pem |
|                        | the server.                                    |                                                      |
+------------------------+------------------------------------------------+------------------------------------------------------+
| *KeyFile*              | Directory where host key is located in the     | KeyFile = /opt/dirac/etc/grid-security/hostcert.pem  |
|                        | server.                                        |                                                      |
+------------------------+------------------------------------------------+------------------------------------------------------+
| *SkipCAChecks*         | Boolean value this attribute allows to express | SkipCAChecks = No                                    |
|                        | if the CA certificates are or not be checked.  |                                                      |
+------------------------+------------------------------------------------+------------------------------------------------------+
| *UseServerCertificate* | Use server certificate, expressed as boolean.  | UseServerCertificate = yes                           |
+------------------------+------------------------------------------------+------------------------------------------------------+

**This section should only appear in the local dirac.cfg file of each installation, never in the central configuration.**