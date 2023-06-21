=========================================
DIRAC Resources
=========================================

DIRAC Resources are logical entities representing computing resources and
services usually provided by third parties. DIRAC is providing an abstract
layer for various types of such services, e.g. Computing or Storage Elements,
File Catalogs, etc. For each particular kind of service an implementation
is provided and objects representing each service is created using its logical name
by an appropriate Factory.

This section describes how Resources of different types can be used for developing
DIRAC applications

.. toctree::
   :maxdepth: 2

   Catalog
   Computing
   MessageQueues/index
   Storage
