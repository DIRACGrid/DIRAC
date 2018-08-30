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

For every question, or comment, regarding specific development activities,
including suggestion and comments to the `RFC <https://github.com/DIRACGrid/DIRAC/wiki/DIRAC-Requests-For-Comments-%28RFC%29>`_,
the correct forum for is the `dirac-develop <https://groups.google.com/forum/#!forum/diracgrid-develop>`_ google group.
For everything operational, instead, you can write on the `dirac-grid <https://groups.google.com/forum/#!forum/diracgrid-forum>`_
group.


.. toctree::
   :maxdepth: 1

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
   WorkloadManagementSystem/index
   TornadoServices/index