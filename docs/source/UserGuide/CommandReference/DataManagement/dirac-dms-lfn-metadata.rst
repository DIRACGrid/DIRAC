.. _dirac-dms-lfn-metadata:

======================
dirac-dms-lfn-metadata
======================

Obtain replica metadata from file catalogue client.

Usage::

  dirac-dms-lfn-metadata [option|cfgfile] ... LFN ...

Arguments::

  LFN:      Logical File Name or file containing LFNs

Example::

  $ dirac-dms-lfn-metadata /formation/user/v/vhamar/Example.txt
  {'Failed': {},
   'Successful': {'/formation/user/v/vhamar/Example.txt': {'Checksum': 'eed20d47',
                                                           'ChecksumType': 'Adler32',
                                                           'CreationDate': datetime.datetime(2011, 2, 11, 14, 52, 47),
                                                           'FileID': 250L,
                                                           'GID': 2,
                                                           'GUID': 'EDE6DDA4-3344-3F39-A993-8349BA41EB23',
                                                           'Mode': 509,
                                                           'ModificationDate': datetime.datetime(2011, 2, 11, 14, 52, 47),
                                                           'Owner': 'vhamar',
                                                           'OwnerGroup': 'dirac_user',
                                                           'Size': 34L,
                                                           'Status': 1,
                                                           'UID': 2}}}
