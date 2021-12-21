.. _externals_support:

===================================
Supported environments and packages
===================================

OS:
---

DIRAC *server* installation is possible for all architectures supported by DIRACOS2 releases: https://github.com/DIRACGrid/DIRACOS2/releases.
DIRAC brings its own version of python at installation time, right now 3.9.x.

MySQL versions:
---------------

MySQL is a hard dependency for all DIRAC servers installations. Supported versions:

- MySQL 5.7
- MySQL 8.0
- MariaDB versions "compatible" with the above MySQL versions.

MySQL server is not shipped with DIRAC. You are responsible of its administration.

While specific configurations may be applied for each MySQL Database connection,
generic connection details can be applied in CS location below (the shown values are the defaults)::

   Systems
   {
     Databases
     {
       User = Dirac
       Password = Dirac
       Host = localhost
       Port = 3306
     }
   }


ElasticSearch versions:
-----------------------

ElasticSearch is an optional dependency for DIRAC servers installations. Supported versions:

- 7.x

ElasticSearch server is not shipped with DIRAC. You are responsible of its administration.

You can run your ES cluster without authentication, or using User name and password, or using certificates. You may add the following parameters:

  - ``User`` (default:'')
  - ``Password`` (default:'')
  - ``Host`` (default:localhost)
  - ``Port`` (default:9201)
  - ``SSL`` (default:True)
  - ``CRT`` (default:True)
  - ``ca_certs`` (default:None)
  - ``client_key`` (default:None)
  - ``client_cert`` (default:None)


to the location::

   Systems
   {
     NoSQLDatabases
     {
       User = ...
       Password = ...
       ...
     }
   }
