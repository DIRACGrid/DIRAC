Systems / Framework / <INSTANCE> / Service / SecurityLogging - Sub-subsection
=============================================================================

SecurityLogging service can be used by all services to log all connections, for security-related purpose.
It can be disabled globally via flag ``/Operations/<VO>/<Setup|Deaults>/EnableSecurityLogging``, or per-service.

+-----------------+------------------------------------------+---------------------------------+
| **Name**        | **Description**                          | **Example**                     |
+-----------------+------------------------------------------+---------------------------------+
| *DataLocation*  | Directory where log info is kept         | DataLocation = data/securityLog |
+-----------------+------------------------------------------+---------------------------------+
