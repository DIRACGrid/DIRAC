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
  - `Externals <https://github.com/DIRACGrid/Externals>`_ is the OLD repository for the DIRAC dependencies, going to be superseded by DIRACOS
  - `VMDIRAC <https://github.com/DIRACGrid/VMDIRAC>`_ is a DIRAC extension for using cloud sites
  - `COMDIRAC <https://github.com/DIRACGrid/COMDIRAC>`_ is a DIRAC extension of its CLI
  - `RESTDIRAC <https://github.com/DIRACGrid/RESTDIRAC>`_ is a DIRAC extension that creates a REST layer for DIRAC services
  - `DB12 <https://github.com/DIRACGrid/DB12>`_ is *not* a DIRAC extension, but a self-contained quick benchmark

The content of the other repositories at `https://github.com/DIRACGrid` have either been included in those above, or became obsolete.

This work must be supported by a suitable development model which
is described in this chapter.

The DIRAC code development is done with the help of the Git code management system.
It is inherently distributed and is well suited for the project. It is outlined
the :ref:`git_management` subsection.

The DIRAC Development Model relies on the excellent Git capability for managing
code branches which is mandatory for a distributed team of developers.
The DIRAC branching model is following strict conventions described in :ref:`branching_model`
subsection.

The DIRAC code management is done using the `Github service <https://github.com/DIRACGrid>`_
as the main code repository. The service provides also facilities for bug and task tracking,
Wiki engine and other tools to support the group code development. Setting up the
Git based development environment and instructions to contribute new code is described
in :ref:`contributing_code` subsection.

The DIRAC releases are described using a special configuration file and tools are provided
to prepare code distribution tar archives. The tools and procedures to release the DIRAC software
are explained in :ref:`release_procedure` subsection.

.. toctree::
   :maxdepth: 1

   GitCodeManagement/index
   BranchingModel/index
   ContributingCode/index
