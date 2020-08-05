.. _dirac-dms-get-file:

==================
dirac-dms-get-file
==================

Retrieve a single file or list of files from Grid storage to the current directory.

Usage::

  dirac-dms-get-file [option|cfgfile] ... LFN ...

Arguments::

  LFN:      Logical File Name or file containing LFNs

Example::

  $ dirac-dms-get-file /formation/user/v/vhamar/Example.txt
  {'Failed': {},
   'Successful': {'/formation/user/v/vhamar/Example.txt': '/afs/in2p3.fr/home/h/hamar/Tests/DMS/Example.txt'}}
