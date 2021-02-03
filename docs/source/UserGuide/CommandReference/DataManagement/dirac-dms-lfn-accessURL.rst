.. _dirac-dms-lfn-accessURL:

=======================
dirac-dms-lfn-accessURL
=======================

Retrieve an access URL for an LFN replica given a valid DIRAC SE.

Usage::

  dirac-dms-lfn-accessURL [option|cfgfile] ... LFN SE [PROTO]

Arguments::

  LFN:      Logical File Name or file containing LFNs (mandatory)
  SE:       Valid DIRAC SE (mandatory)
  PROTO:    Optional protocol for accessURL

Example::

  $ dirac-dms-lfn-accessURL /formation/user/v/vhamar/Example.txt DIRAC-USER
  {'Failed': {},
   'Successful': {'/formation/user/v/vhamar/Example.txt': 'dips://dirac.in2p3.fr:9148/DataManagement/StorageElement   /formation/user/v/vhamar/Example.txt'}}
