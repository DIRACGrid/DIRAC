.. _externals_support:

===================================
Supported environments and packages
===================================

OS
--

DIRAC needs `DIRACOS <https://github.com/DIRACGrid/DIRACOS2/releases>`_ for its dependencies. DIRACOS includes all dependencies (except for glibc), including Python and Grid middleware.

DIRAC *client* installation is supported for all x86_64, aarch64, ppc64le Linux and Darwin installations using `DIRACOS <https://github.com/DIRACGrid/DIRACOS2/releases>`_. This includes installations made by the pilots.

DIRAC *server* installation is supported for *most* x86_64 Linux installations using `DIRACOS <https://github.com/DIRACGrid/DIRACOS2/releases>`_. Other architectures and platforms may work as a server installation, however this is on a best effort basis and is not regularly tested.

MySQL versions
--------------

MySQL is a hard dependency for all DIRAC servers installations. Supported versions:

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


ElasticSearch versions
----------------------

ElasticSearch is an optional dependency for DIRAC servers installations. Supported versions:

- 7.x up until 7.13
- OpenDistro and OpenSearch releases "compatible" with the above ElasticSearch versions.

ElasticSearch server is not shipped with DIRAC. You are responsible of its administration.

You can run your ES cluster without authentication, or using User name and password, or using certificates. You may add the following parameters:

  - ``User`` (default:``''``)
  - ``Password`` (default:``''``)
  - ``Host`` (default:``localhost``)
  - ``Port`` (default:``9201``)
  - ``SSL`` (default:``True``)
  - ``CRT`` (default:``True``)
  - ``ca_certs`` (default:``None``)
  - ``client_key`` (default:``None``)
  - ``client_cert`` (default:``None``)


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
