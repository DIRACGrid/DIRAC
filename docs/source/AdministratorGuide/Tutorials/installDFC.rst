.. _tuto_install_dfc:

=================================
Installing the DIRAC File Catalog
=================================

.. set highlighting to console input/output
.. highlight:: console

Pre-requisite
=============

You should:

* have a machine setup as described in :ref:`tuto_basic_setup`
* be able to install dirac components
* have installed a DIRAC SE using the tutorial (:ref:`tuto_install_dirac_se`).

Tutorial goal
=============

The aim of the tutorial is to install the DIRAC FileCatalog (DFC)
By the end of the tutorial, you will be able to do all sort of simple Data Management operations.

More links
==========

More information can be found at the following places:

* Introduction to DataManagement: :ref:`data-management-system`
* Catalog resource definition :ref:`resourcesCatalog`
* How-to datamanagement for user :ref:`howto_user_dms`

Installing the DFC
==================

This section is to be executed as ``diracuser`` with a proxy with ``dirac_admin`` group.

The DFC is no different than any other DIRAC service with a database. The installation step are thus very simple::

  [diracuser@dirac-tuto ~]$ dirac-admin-sysadmin-cli --host dirac-tuto
  Pinging dirac-tuto...
  [dirac-tuto]$ install db FileCatalogDB
  Adding to CS DataManagement/FileCatalogDB
  Database FileCatalogDB from DIRAC/DataManagementSystem installed successfully
  [dirac-tuto]$ install service DataManagement FileCatalog
  Loading configuration template /home/diracuser/DIRAC/DIRAC/DataManagementSystem/ConfigTemplate.cfg
  Adding to CS service DataManagement/FileCatalog
  service DataManagement_FileCatalog is installed, runit status: Run


Adding the FileCatalog resource
===============================

In order to be used as a FileCatalog by clients, the DFC needs to be declared. This happens in two places:

* ``/Resources/FileCatalogs/``: in this section, you define how to access the catalog
* ``/Operations/Defaults/Services/Catalogs/``: in this section, you define how to use the catalog (for example read/write)


Since we have only one catalog, we will use it as ``Read-Write`` and as ``Master``.

Using the WebApp (group ``dirac_admin``), add the following in ``/Resources/FileCatalogs/`` (all options to defaults)::

    FileCatalog
    {
    }


Using the WebApp, add the following in ``/Operations/Defaults/Services/Catalogs``::

  FileCatalog
  {
    AccessType = Read-Write
    Status = Active
    Master = True
  }

From this moment onward, the catalog is totally usable.

Test the catalog
================

Since we have a StorageElement at our disposal, we can use the standard ``dirac-dms-*`` script.

First, let us create a file and then "put it on the grid"::


  [diracuser@dirac-tuto ~]$ echo "Hello" > /tmp/world.txt
  [diracuser@dirac-tuto ~]$ dirac-dms-add-file /tutoVO/user/c/ciuser/world.txt /tmp/world.txt StorageElementOne

  Uploading /tutoVO/user/c/ciuser/world.txt
  Successfully uploaded file to StorageElementOne


Now, let's check its replicas and metadata::

  [diracuser@dirac-tuto ~]$ dirac-dms-lfn-replicas /tutoVO/user/c/ciuser/world.txt
  LFN                             StorageElement    URL
  =====================================================
  /tutoVO/user/c/ciuser/world.txt StorageElementOne dips://dirac-tuto:9148/DataManagement/StorageElement/tutoVO/user/c/ciuser/world.txt

  [diracuser@dirac-tuto ~]$ dirac-dms-lfn-metadata /tutoVO/user/c/ciuser/world.txt
  {'Failed': {},
  'Successful': {'/tutoVO/user/c/ciuser/world.txt': {'Checksum': '078b01ff',
                                                      'ChecksumType': 'Adler32',
                                                      'CreationDate': datetime.datetime(2019, 4, 16, 9, 5, 58),
                                                      'FileID': 1L,
                                                      'GID': 1,
                                                      'GUID': '09F7E02F-1290-BE21-1DA7-07A266F153B3',
                                                      'Mode': 509,
                                                      'ModificationDate': datetime.datetime(2019, 4, 16, 9, 5, 58),
                                                      'Owner': 'ciuser',
                                                      'OwnerGroup': 'dirac_admin',
                                                      'Size': 6L,
                                                      'Status': 'AprioriGood',
                                                      'UID': 1}}}

Note that these metadata are those registered in the catalog (which hopefully should match the physical one !)

We can also check all the user files that belong to us on the grid::

  [diracuser@dirac-tuto ~]$ dirac-dms-user-lfns
  Will search for files in /tutoVO/user/c/ciuser
  /tutoVO/user/c/ciuser: 1 files, 0 sub-directories
  1 matched files have been put in tutoVO-user-c-ciuser.lfns
  [diracuser@dirac-tuto ~]$ cat tutoVO-user-c-ciuser.lfns
  /tutoVO/user/c/ciuser/world.txt

Finally, let's remove the file::

  [diracuser@dirac-tuto ~]$ dirac-dms-remove-files /tutoVO/user/c/ciuser/world.txt
  Successfully removed 1 files
