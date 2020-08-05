.. _data-management-system:

======================
Data Management System
======================



The DIRAC Data Management System (DMS), together with the DIRAC Storage Management System (SMS) provides the necessary functionality to execute and control all activities related with your data. the DMS provides from the basic functionality to upload a local file in a StorageElement (SE) and register the corresponding replica in the FileCatalog (FC) to massive data replications using FTS or retrievals of data archived on Tape for it later processing.

To achieve this functionality the DMS and SMS require a proper description of the involved external servers (SE, FTS, etc.) as well as a number of Agents and associated Servers that animate them. In the following sections the different aspects of each functional component are explained in some detail.


.. toctree::
   :maxdepth: 1

   concepts
   dfc
   fts3
   s3
