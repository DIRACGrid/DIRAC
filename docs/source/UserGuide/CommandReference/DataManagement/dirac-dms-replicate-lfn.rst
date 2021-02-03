.. _dirac-dms-replicate-lfn:

=======================
dirac-dms-replicate-lfn
=======================

Replicate an existing LFN to another Storage Element

Usage::

  dirac-dms-replicate-lfn [option|cfgfile] ... LFN Dest [Source [Cache]]

Arguments::

  LFN:      Logical File Name or file containing LFNs (mandatory)
  Dest:     Valid DIRAC SE (mandatory)
  Source:   Valid DIRAC SE
  Cache:    Local directory to be used as cache

Example::

  $ dirac-dms-replicate-lfn /formation/user/v/vhamar/Test.txt DIRAC-USER
  {'Failed': {},
   'Successful': {'/formation/user/v/vhamar/Test.txt': {'register': 0.50833415985107422,
                                                        'replicate': 11.878520965576172}}}
