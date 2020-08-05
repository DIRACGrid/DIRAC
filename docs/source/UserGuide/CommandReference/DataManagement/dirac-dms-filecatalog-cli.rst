.. _dirac-dms-filecatalog-cli:

=========================
dirac-dms-filecatalog-cli
=========================

Launch the File Catalog shell

Usage::

   dirac-dms-filecatalog-cli [option]

Options::

  -f  --file-catalog <value>   :    Catalog client type to use (default FileCatalog)

Example::

  $ dirac-dms-filecatalog-cli
  Starting DIRAC FileCatalog client
  File Catalog Client $Revision: 1.17 $Date:
  FC:/>help

  Documented commands (type help <topic>):
  ========================================
  add    chmod  find   guid  ls     pwd       replicate  rmreplica   user
  cd     chown  get    id    meta   register  rm         size
  chgrp  exit   group  lcd   mkdir  replicas  rmdir      unregister

  Undocumented commands:
  ======================
  help

  FC:/>
