.. _externals_support:

===================================
Supported environments and packages
===================================

OS:
---

DIRAC *server* installation is officially supported only for:

- CERN CentOS 7 (`CC7 <https://linux.web.cern.ch/centos7/>`_)

Tests are also regularly run for other platforms but no official support is offered for them.


Python versions:
----------------

DIRAC brings its own version of python at installation time: this version is right now fixed to 2.7.13.
An experimental support for python3 server installations is also offered from DIRAC version 7.3.

DIRAC installation scripts (including pilots) use the native python version, and for this the python versions supported are:

- 2.6.9 (native on SLC6)
- 2.7.5 (native on CC7)
- 2.7.13
- 3.6.8+

MySQL versions:
---------------

MySQL is a hard dependency for all DIRAC servers installations. Supported versions:

- 5.7
- 8.0

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

- 6.x
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

