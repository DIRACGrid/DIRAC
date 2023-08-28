.. _developer_guide:

===============
Developer Guide
===============

The DIRAC Developer Guide is describing procedures, rules and practical details for developing
new DIRAC components. The section :ref:`development_model` describes the general code management
procedures, building and distribution of the DIRAC releases.

To work on the code, DIRAC developers need to set up an environment to work on the software
components and to test it together with other parts of the distributed system. Setting up
such an environment is discussed in :ref:`development_environment`.

An overview of the DIRAC software architecture is presented in the :ref:`dirac_overview` section.
Detailed instructions on how to develop various types of DIRAC components are given in
:ref:`adding_new_components` chapter. It gives examples with explanations, common utilities
are discussed as well. More detailes on the available interfaces can be found in the
:ref:`code_documentation` part.

For issues, please open a `GitHub issue <https://github.com/DIRACGrid/DIRAC/issues>`_.
For questions, comments, or operational issues, use `GitHub discussions <https://github.com/DIRACGrid/DIRAC/discussions>`_.

.. toctree::
   :maxdepth: 1

   ReleaseProcedure/index
   DevelopmentModel/index
   DevelopmentEnvironment/index
   Overview/index
   CodingConvention/index
   AddingNewComponents/index
   CodeDocumenting/index
   CodeTesting/index
   Systems/index
   REST/index
   WebAppDIRAC/index
   Internals/index
   Externals/index
   TornadoServices/index
   APIs/index
   OAuth2Authorization/index
