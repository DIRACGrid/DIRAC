.. _dirac-wms-get-normalized-queue-length:

=====================================
dirac-wms-get-normalized-queue-length
=====================================

Report Normalized CPU length of queue

Usage::

  dirac-wms-get-normalized-queue-length [option|cfgfile] ... Queue ...

Arguments::

  Queue:     GlueCEUniqueID of the Queue (ie, juk.nikhef.nl:8443/cream-pbs-lhcb)

Example::

  $ dirac-wms-get-normalized-queue-length  cclcgceli03.in2p3.fr:2119/jobmanager-bqs-long
  cclcgceli03.in2p3.fr:2119/jobmanager-bqs-long 857400.0
