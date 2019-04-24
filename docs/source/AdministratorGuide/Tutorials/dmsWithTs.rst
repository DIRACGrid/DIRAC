===============================================================
Doing large scale DataManagement with the Transformation System
===============================================================

Pre-requisite
=============

Pre-requisite
=============

You should:

* have a machine setup as described in :ref:`tuto_basic_setup`
* have installed two DIRAC SE using the tutorial (:ref:`tuto_install_dirac_se`).
* have installed the DFC using the tutorial (:ref:`tuto_install_dfc`).
* have followed the tutorial on identity management (:ref:`tuto_managing_identities`)
* have installed the RMS using the tutorial (:ref:`tuto_install_rms`)


Tutorial goal
=============

The aim of the tutorial is to demonstrate how large scale data management operations (removals, replications, etc) can be achieved using the Transformation System.
By the end of the tutorial, you will be able to:

* Submit simple transformation for manipulating a given list of files
* Have transformations automatically fed thanks to metadata
* Write your own plugin for TransformationSystem


More links
==========

* :ref:`adminTS`


Installing the RequestManagementSystem
======================================

This section is to be performed as ``diracuser`` with a proxy in ``dirac_admin`` group.

In order to have asynchronous operations handled in DIRAC, you need to have the RequestManagementSystem installed. For it to be functional, you need at least:

* The ReqManager: the service interfacing this system
* The RequestExecutingAgent: the agent performing the operations
