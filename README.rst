DIRAC
=====
Introduction goes here


Installing
==========
DIRAC provides a ``setuptools`` script for development, building and installing the package(s).
To install DIRAC, do::

    $ python setup.py install

The standard set of ``setuptools`` command are supplied, so tests may be run by doing::

    $ python setup.py test

Note that at present not all tests will pass, as the full dependency tree
of DIRAC has not yet been resolved for inclusion as ``setuptools`` requires
statements.

Requirements
============
These are skimmed from `Link the DIRACGRID Externals project https://github.com/DIRACGrid/Externals`.
It should only be regarded as preliminary in this fork and may not be in sync
with the requirements listed in the ``setup.py`` file.

* client tools
  * Python 2.7 (need to determine valid Python versions) with support for
    * readline
    * bzip2
    * zlib
    * ncurses
    * readline
    * openssl
  * simplejson 3.8.1
  * fuse-python 0.2
  * pyparsing 2.0.6
  * pyGSI
* server tools
  * everything in client tools
  * ldap 
    * openldap 2.4.23
    * python-ldap 2.3.10)
  * runit 2.1.1
  * serverLibReqs
    * libart_lgpl 2.3.20
    * freetype 2.3.11
    * libpng 1.2.40
  * rrdtool 1.4.9
  * pyPlotTools
    * Imaging 1.1.6
    * matplotlib 1.5.0
    * numpy 1.10.1
    * pytz 2015.7
  * ServerPackages
    * sqlalchemy 1.0.9
    * pexpect 4.0.1
    * MySQL-python 1.2.5
    * requests 2.9.1
  * SOAP
    * suds 0.4
    * boto 1.9b
  * WebModules
    * WebOb 0.9.6.1
    * Pylons 0.9.7
    * flub 1.0
    * webtest 1.4.3
  * WebServer
    * pcre 8.01
    * lighttpd 1.4.28

