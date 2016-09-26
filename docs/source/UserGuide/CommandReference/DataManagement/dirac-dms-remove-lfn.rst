===========================
dirac-dms-remove-lfn
===========================

  Remove LFN and *all* associated replicas from Storage Elements and File Catalogs.

Usage::

  dirac-dms-remove-lfn [option|cfgfile] ... LFN ...

Arguments::

  LFN:      Logical File Name or file containing LFNs 

Example::

  $ dirac-dms-remove-lfn  /formation/user/v/vhamar/0/16/StdOut
  {'Failed': {},
   'Successful': {'/formation/user/v/vhamar/0/16/StdOut': {'FileCatalog': True}}}


