--------------
FileCatalog
--------------

The full code documentation is available here :py:class:`~DIRAC.Resources.Catalog.FileCatalog.FileCatalog`

The `FileCatalog` relies on plugins to actually perform the operations, and will just loop over them

How to use it
-------------

.. warning::

   `FileCatalog` class should only be used when no interactions with the Storages are expected. Typically, using FileCatalog to add new files without copying them will lead to lost data. If you want consistency between both, use the DataManager class


.. code-block:: python

   # Necessary import
   from DIRAC.Resources.Catalog.FileCatalog import FileCatalog

   # Instanciate a FileCatalog
   fc = FileCatalog()

   # LFNs to use as example
   lfns = ['/lhcb/user/c/chaen/zozo.xml']
   directory  = ['/lhcb/data/']

   # Get the namespace metadata
   fc.getFileMetadata(lfns)

   # {'OK': True,
   #  'Value': {'Failed': {},
   #   'Successful': {'/lhcb/user/c/chaen/zozo.xml': {'Checksum': '29eddd7b',
   #     'ChecksumType': 'Adler32',
   #     'CreationDate': datetime.datetime(2015, 1, 23, 10, 28, 2),
   #     'FileID': 171670021L,
   #     'GID': 1470L,
   #     'GUID': 'ECEC10C9-E7F3-36CA-8935-A9B483E97D2C',
   #     'Mode': 436,
   #     'ModificationDate': datetime.datetime(2015, 1, 23, 10, 28, 2),
   #     'Owner': 'chaen',
   #     'OwnerGroup': 'lhcb',
   #     'Size': 769L,
   #     'Status': 'AprioriGood',
   #     'UID': 20269L}}}}


   # Listing a directory
   fc.listDirectory(directory)

   # {'OK': True,
   #  'Value': {'Failed': {},
   #   'Successful': {'/lhcb/data/': {'Datasets': {},
   #     'Files': {},
   #     'Links': {},
   #     'SubDirs': {'/lhcb/data/2008': True,
   #      '/lhcb/data/2009': True,
   #      '/lhcb/data/2010': True,
   #      '/lhcb/data/2011': True,
   #      '/lhcb/data/2012': True,
   #      '/lhcb/data/2013': True,
   #      '/lhcb/data/2014': True,
   #      '/lhcb/data/2015': True,
   #      '/lhcb/data/2016': True,
   #      '/lhcb/data/2017': True,
   #      '/lhcb/data/2018': True}}}}}


Adding a new Catalog
--------------------

The best doc for the time being is to look at an example like :py:class:`~DIRAC.Resources.Catalog.FileCatalogClient.FileCatalogClient`
