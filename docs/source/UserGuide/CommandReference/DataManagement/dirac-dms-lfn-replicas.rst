.. _dirac-dms-lfn-replicas:

======================
dirac-dms-lfn-replicas
======================

Obtain replica information from file catalogue client.

Usage::

  dirac-dms-lfn-replicas [option|cfgfile] ... LFN ...

Arguments::

  LFN:      Logical File Name or file containing LFNs

Options::

  -a  --All                    :   Also show inactive replicas

Example::

  $ dirac-dms-lfn-replicas /formation/user/v/vhamar/Test.txt
  {'Failed': {},
   'Successful': {'/formation/user/v/vhamar/Test.txt': {'M3PEC-disk': 'srm://se0.m3pec.u-bordeaux1.fr/dpm/m3pec.u-bordeaux1.fr/home/formation/user/v/vhamar/Test.txt'}}}
