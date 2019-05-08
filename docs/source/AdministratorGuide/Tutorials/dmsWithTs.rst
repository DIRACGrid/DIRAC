===============================================================
Doing Large Scale DataManagement with the Transformation System
===============================================================

Pre-Requisite
=============

You should:

* have a machine setup as described in :ref:`tuto_basic_setup`
* have installed two DIRAC SE using the tutorial (:ref:`tuto_install_dirac_se`).
* have installed the DFC using the tutorial (:ref:`tuto_install_dfc`).
* have followed the tutorial on identity management (:ref:`tuto_managing_identities`)
* have installed the RMS using the tutorial (:ref:`tuto_install_rms`)
* have installed the TS using the tutorial (:ref:`tuto_install_ts`)


Tutorial Goal
=============

The aim of the tutorial is to demonstrate how large scale data management operations (removals, replications, etc.) can
be achieved using the Transformation System.  By the end of the tutorial, you will be able to:

* Submit simple transformation for manipulating a given list of files
* Have transformations automatically fed thanks to metadata
* Write your own plugin for the TransformationSystem


More Links
==========

* :ref:`adminTS`


Creating a Replication Transformation
=====================================

This section is to be performed as ``diracuser`` with a proxy in ``dirac_data`` group.

First we need to create some files and upload them to ``StorageElementOne``::

  for ID in {1..10}; do echo "MyContent $ID" > File_${ID} ; dirac-dms-add-file /tutoVO/data/Trans_01/File_${ID} File_${ID} StorageElementOne ; done

Then we create the list of LFNs we just uploaded::

  dirac-dms-user-lfns -b /tutoVO/data/Trans_01
  Will search for files in /tutoVO/data/Trans_01
  /tutoVO/data/Trans_01: 10 files, 0 sub-directories
  10 matched files have been put in tutoVO-data-Trans_01.lfns

Now we can simply create a transformation to replicate files from StorageElementOne to StorageElementTwo::

  dirac-transformation-replication 1 StorageElementTwo -S StorageElementOne -x
  Created transformation NNN
  Successfully created replication transformation

This created transformation with the unique transformation ID *NNN* (e.g., 1).

By default this transformation used *MetaData* information to obtain the input files using the *InputDataAgent*. Instead
we can also just add files manually using the list we created previously, replace NNN by the ID of the transformation
that was just created::

  dirac-transformation-add-files NNN tutoVO-data-Trans_01.lfns
  Successfully added 10 files


Now we have to wait until the ``TransformationAgent`` runs again and creates a *Task* for each of the files. Once the
tasks are created, the *RequestTaskAgent* creates a request out of each task, which is then processed in the
*RequestExecutingAgent* of the RMS.
