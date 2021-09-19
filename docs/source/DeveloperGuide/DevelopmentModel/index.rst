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

The DIRAC python2 releases are described using a special configuration file and tools are provided
to prepare code distribution tar archives. The tools and procedures to release the DIRAC software
are explained in :ref:`release_procedure` subsection.

The DIRAC python3 releases use instead standard python tools.

DIRAC releases nomenclature
-----------------------------

Python2 Release version name conventions
@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@

The DIRAC release versions have the form ``vXrYpZ``, where ``X``, ``Y`` and ``Z`` are incrementally
increasing interger numbers. For example, v1r0p1. ``X`` corresponds to the major release number,
``Y`` corresponds to the minor release number and ``Z`` corresponds to the patch number ( see below ).
It is possible that the patch number is not present in the release version, for example v6r7 .

The version of prereleases used in the DIRAC certification procedure is constructed in a form:
``vXrY-preZ``, where the ``vXrY`` part corresponds to the new release being tested and ``Z``
denotes the prerelease number.

Release versions are used as tags in Git terms to mark the set of codes corresponding to the
given release.

Release types
@@@@@@@@@@@@@@

We distinguish *releases*, *patches* and *pre-releases*. Releases in turn can be *major* and *minor*.

*major release*
  major releases are created when there is an important change in the DIRAC functionality involving
  changes in the service interfaces making some of the previous major release clients incompatible
  with the new services. DIRAC clients and services belonging to the same major release are still
  compatible even if they belong to different normal releases. In the release version the major
  release is denoted by the number following the initial letter "v".

*minor release*
  minor releases are created whenever a significant new functionality is added or important changes
  of the existing functionality are done. Minor releases necessitate certification step in order to make
  the new code available in production for users. In the release version minor releases are denoted
  by the number following the letter "r".

*patch*
  patches are created for a minor and/or obvious bug fixes and minor functionality changes. This
  is the responsibility of the developer to ensure that the patch changes only fix known problems
  and do not introduce new bugs or undesirable side effects. Patch releases are not subject to the
  full release certification procedure. Patches are applied to the existing releases. In the release
  version patch releases are denoted by the number following the letter "p".

*pre-release*
  the DIRAC certification procedure goes through a series of *pre-releases* used to test carefully the
  code to be released in production. The prerelease versions have a form ``vXrY-preZ``.

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
