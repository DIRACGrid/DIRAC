--------------
StorageElement
--------------

The full code documentation is available here :py:class:`~DIRAC.Resources.Storage.StorageElement.StorageElementItem`

The `StorageElement` relies on plugins to actually perform the operations, and will just loop over them

How to use it
-------------

.. warning::

   `StorageElement` class should only be used when no interactions with the Catalogs are expected. Typically, using StorageElement to add new files without registering them will lead to dark data. If you want consistency between both, use the DataManager class



.. code-block:: python

   # Necessary import
   from DIRAC.Resources.Storage.StorageElement import StorageElement

   # Instanciate a StorageElement
   se = StorageElement('CERN-USER')

   # LFNs to use as example
   lfns = ['/lhcb/user/c/chaen/zozo.xml']

   # Get the physical metadata
   se.getFileMetadata(lfns)

   # {'OK': True,
   #  'Value': {'Failed': {},
   #   'Successful': {'/lhcb/user/c/chaen/zozo.xml': {'Accessible': True,
   #     'Checksum': '29eddd7b',
   #     'Directory': False,
   #     'Executable': False,
   #     'File': True,
   #     'FileSerialNumber': 51967,
   #     'GroupID': 1470,
   #     'LastAccess': '2017-07-25 15:06:19',
   #     'Links': 1,
   #     'ModTime': '2017-07-25 15:06:19',
   #     'Mode': 256,
   #     'Readable': True,
   #     'Size': 769L,
   #     'StatusChange': '2017-07-25 15:06:19',
   #     'UserID': 56212,
   #     'Writeable': False}}}}

   # Get the URL, using operation defaults for the protocol
   se.getURL(lfns)

   # {'OK': True,
   #  'Value': {'Failed': {},
   #   'Successful': {'/lhcb/user/c/chaen/zozo.xml': 'root://eoslhcb.cern.ch//eos/lhcb/grid/user/lhcb/user/c/chaen/zozo.xml'}}}

   # Specify the protocol to use
   se.getURL(lfns, protocol = 'srm')

   # {'OK': True,
   #  'Value': {'Failed': {},
   #   'Successful': {'/lhcb/user/c/chaen/zozo.xml': 'srm://srm-eoslhcb.cern.ch:8443/srm/v2/server?SFN=/eos/lhcb/grid/user/lhcb/user/c/chaen/zozo.xml'}}}


Adding a new plugin/protocol
----------------------------

The best doc for the time being is to look at an example like :py:class:`~DIRAC.Resources.Storage.GFAL2_XROOTStorage.GFAL2_XROOTStorage`
