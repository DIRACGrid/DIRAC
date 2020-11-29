=======================
Configuration System
=======================

The configuration file from DIRAC server is located under::

   $DIRAC_ROOT_PATH/etc/<Conf Name>.cfg

This file is divided in sections and subsections.

A similar tree with the description of all the attributes is tried to be represented in this help tree.

The detailed configuration options for agents, services, and executors are in the process of being migrated to the
:ref:`code_documentation`, so also see in the relevant module documentation there.

.. toctree::
   :maxdepth: 1

   DiracSection
   Operations/index
   Resources/index
   Systems/index
   WebSite/index
   Tips/index
   

Note: This configuration file can be edited by hand, but we strongly recommend you to configure using DIRAC Web Portal.
