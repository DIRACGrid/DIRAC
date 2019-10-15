.. dedicatedDFC:

========================================
Installing a Dedicated DIRAC FileCatalog
========================================

This HowTo explains the steps needed to install an additional instance of the DIRAC FileCatalog, which uses a separated
Database.

For this to work one needs a separate database, which will be assumed to be called NFCDB from now on.
Using the :ref:`admin_dirac-admin-sysadmin-cli` does not allow one to add NFCDB, so it has to be done manually.

Prepare the CS for the new Database
------------------------------------

First the information for the new database is added to the Configuration System. All the parameters in the
``Systems/DataManagement/<SETUP>/Databases/NFCDB`` section can be copied from the ``FileCatalogDB`` section, except that
the ``DBName`` has to be pointing to the soon to be created database::

  DBName = NFCDB

``Host`` and ``Port`` of the MySQL server can be the same as for ``FileCatalogDB``, but can also be different if the
database is hosted on a different server.


Fill the new Database with the Necessary Tables
-----------------------------------------------

Now the database has to be created on the MySQL server and the ``Dirac`` user has to be granted the proper permissions.

The database name has to be changed in the ``sql`` file used to create all the tables needed by the Dirac FileCatalog. In the shell::

  wget https://raw.githubusercontent.com/DIRACGrid/DIRAC/integration/DataManagementSystem/DB/FileCatalogDB.sql -O NFCDB.sql
  sed -i s/FileCatalogDB/NFCDB/g NFCDB.sql

on the MySQL server as ``root`` or ``admin``::

  create database NFCDB;

and grant the Dirac user the necessary permissions on NFCDB. See The beginning of NFCDB.sql for the necessary
permissions. The proper syntax for the GRANT statements depends on the MySQL version.

create all the tables in the database::

  mysql -U Dirac -p < NFCDB.sql

.. note:: If this does not work the dirac user has insufficient permissions on the NFCDB, maybe run it as root/admin, but do not
          forget to ``Grant`` the ``dirac`` user the permissions on the database (There are no clear error messages when that is
          forgotten, the service will just appear to hang.


Install the DFC Service
-----------------------

Install the new Dirac FileCatalog service.  The ``Database`` option has to be set to ``DataManagement/NFCDB`` or just
NFCDB, because ``DataManagement`` is pre-pended.

in the :ref:`admin_dirac-admin-sysadmin-cli`::

  install service DataManagement NewFileCatalog -p Port=9198 -p Database=NFCDB

The service options, and also the ``URL`` section should be re-viewed for the value used for the new service.

  
Configuration Settings
----------------------

The ``NewFilecatalog`` has to be added in the configuration section ``Resources/FileCatalogs``::

   NewFileCatalog
   {
     CatalogType = FileCatalog
     CatalogURL = DataManagement/NewFileCatalog
   }

See also the documentation on :ref:`resourcesCatalog`.

The ``NewFileCatalog`` section has to be added in the VO dependent section of the ``Operations`` section so that
`NewFilecAtalog`` is only picked up when using this VO.  The ``CatalogList`` option has to be set as well, to define
which catalog to use for the THEVO, otherwise it will use all catalogs
``Operations/<THEVO>/Defaults/Services/Catalogs/CatalogList = NewFileCatalog``.

E.g.::

  Operations
  {
    THEVO
    {
      Defaults
      {
        Services
        {
          Catalogs
          {
            CatalogList = NewFileCatalog
            NewFileCatalog
            {
              AccessType = Read-Write
              Status = Active
              Master = True
            }
          }
        }
      }
    }


Testing the new FileCatalog
---------------------------

The command :ref:`dirac-dms-filecatalog-cli` does not pick up the FileCatalog from Operations by itself at the moment
(v7r0p2), but it can be told filecatalog to use:

  dirac-dms-filecatalog-cli -f NewFileCatalog -ddd

If everything worked one should see in the debug output that the NewFileCatalog is accessed

The command :ref:`dirac-dms-add-file` does figure out from the VO which filecatalog should be used.
