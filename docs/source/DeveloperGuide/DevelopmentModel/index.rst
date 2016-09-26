.. _development_model:

==================================
Development Model
==================================

The DIRAC Project is advanced collectively by a distributed team of 
developers working in parallel on the core software as well as on various 
extensions. This work must be supported by a suitable development model which 
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
   DIRACProjects/index
   ReleaseProcedure/index
   
   
