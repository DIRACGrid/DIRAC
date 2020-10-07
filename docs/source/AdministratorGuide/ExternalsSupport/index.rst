.. _externals_support:

===================================
Supported environments and packages
===================================

OS:
---

DIRAC *server* installation is officially supported for:

- Scientific Linux CERN 6 (`SLC6 <https://linux.web.cern.ch/scientific6/>`_)
- CERN CentOS 7 (`CC7 <https://linux.web.cern.ch/centos7/>`_)

Tests are also regularly run for other platforms but no official support is offered for them.


Python versions:
----------------

DIRAC brings its own version of python at installation time: this version is right now fixed to 2.7.13.

DIRAC installation scripts (including pilots) use the native python version, and for this the python versions supported are:

- 2.6.9 (native on SLC6)
- 2.7.5 (native on CC7)
- 2.7.13
- 3.6.8+

MySQL versions:
---------------

MySQL is a hard dependency for all DIRAC servers installations. Supported versions:

- 5.6
- 5.7
- 8.0


ElasticSearch versions:
-----------------------

ElasticSearch is an optional dependency for DIRAC servers installations. Supported versions:

- 6.x
- 7.x
