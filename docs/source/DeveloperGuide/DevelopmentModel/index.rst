.. _development_model:

==================================
Development Model
==================================

The DIRACGrid Project is a pure open source project, advanced collectively by a distributed team of
developers working in parallel on the core software as well as on various
extensions. Everybody is welcome to participate.

The DIRACGrid project includes several repositories, all hosted in `Github <https://github.com/DIRACGrid>`_:

  - `DIRAC <https://github.com/DIRACGrid/DIRAC>`_ is the main repository: contains the client and server code
  - `WebAppDIRAC <https://github.com/DIRACGrid/WebAppDIRAC>`_ is the repository for the web portal
  - `Pilot <https://github.com/DIRACGrid/Pilot>`_ is *not* a DIRAC extension, but a new version of the DIRAC pilots (dubbed Pilots 3.0)
  - `DIRACOS <https://github.com/DIRACGrid/DIRACOS>`_ is the repository for the DIRAC dependencies
  - `DIRACOS2 <https://github.com/DIRACGrid/DIRACOS2>`_ is the python3 repository for the DIRAC dependencies
  - `COMDIRAC <https://github.com/DIRACGrid/COMDIRAC>`_ is a DIRAC extension of its CLI
  - `DB12 <https://github.com/DIRACGrid/DB12>`_ is *not* a DIRAC extension, but a self-contained quick benchmark
  - `management <https://github.com/DIRACGrid/management>`_ is *not* a DIRAC extension, but a repository for creating docker images used for tests and for creating releases
  - `diraccfg <https://github.com/DIRACGrid/diraccfg>`_ is a stand-alone utility for parsing DIRAC cfg files

The content of the other repositories at `https://github.com/DIRACGrid` have either been included in those above, or became obsolete.

This work must be supported by a suitable development model which
is described in this chapter.

The DIRAC code development is done with the help of the Git code management system.
It is inherently distributed and is well suited for the project. It is outlined in this `guide on github <https://guides.github.com/introduction/flow/>`_

The DIRAC Development Model relies on the excellent Git capability for managing
code branches which is mandatory for a distributed team of developers.

DIRAC releases nomenclature follow `PEP 440 <https://www.python.org/dev/peps/pep-0440/>`_.


Release branches
-------------------------

The following branches are used in managing DIRAC releases:

*integration branch*
  this branch is used to assemble new code developments that will be eventually released as a new major or
  minor release.

*release branches*
  the release branches contain the code corresponding to a given major or minor release. This is the production
  code which is distributed for the DIRAC installations. The release branches are created when a new minor
  release is done. The patches are incorporated into the release branches. The release branch names have the
  form ``rel-vXrY``, where ``vXrY`` part corresponds to the branch minor release version.

*master branch*
  the master branch corresponds to the current stable production code. It is a copy of the corresponding
  release branch.

These branches are the only ones maintained in the central Git repository
by the release managers. They are used to build DIRAC releases. They also serve
as reference code used by developers as a starting point for their work.
