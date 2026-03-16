.. _development_model:

==================================
Development Model for DIRAC
==================================

NB: This document covers DIRAC developments. For DiracX, please check `its documentation <https://diracx.diracgrid.org>`_.

The DIRACGrid Project is an open source project, advanced collectively by a distributed team of developers.

The DIRACGrid project includes several repositories, all hosted in `Github <https://github.com/DIRACGrid>`_:

  - `DIRAC <https://github.com/DIRACGrid/DIRAC>`_ is the main repository: contains the client and server code
  - `WebAppDIRAC <https://github.com/DIRACGrid/WebAppDIRAC>`_ is the repository for the web portal
  - `Pilot <https://github.com/DIRACGrid/Pilot>`_ is *not* a DIRAC extension, but a new version of the DIRAC pilots (dubbed Pilots 3.0)
  - `DIRACOS2 <https://github.com/DIRACGrid/DIRACOS2>`_ is the repository for the DIRAC dependencies (py3)
  - `DB12 <https://github.com/DIRACGrid/DB12>`_ is *not* a DIRAC extension, but a self-contained quick benchmark
  - `management <https://github.com/DIRACGrid/management>`_ is *not* a DIRAC extension, but a repository for creating docker images used for tests and for creating releases
  - `diraccfg <https://github.com/DIRACGrid/diraccfg>`_ is a stand-alone utility for parsing DIRAC cfg files

The content of the other repositories at `https://github.com/DIRACGrid` have either been included in those above, or became obsolete.

DIRAC releases nomenclature follow `PEP 440 <https://www.python.org/dev/peps/pep-0440/>`_.
DIRAC uses `Trunk Based Development <https://trunkbaseddevelopment.com>`_ and uses the `integration` branch as trunk.

