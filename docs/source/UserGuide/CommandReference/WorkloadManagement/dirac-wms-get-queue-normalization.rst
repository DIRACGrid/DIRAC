.. _dirac-wms-get-queue-normalization:

=================================
dirac-wms-get-queue-normalization
=================================

Report Normalization Factor applied by Site to the given Queue

Usage::

  dirac-wms-get-queue-normalization [option|cfgfile] ... Queue ...

Arguments::

  Queue:     GlueCEUniqueID of the Queue (ie, juk.nikhef.nl:8443/cream-pbs-lhcb)

Example::

  $ dirac-wms-get-queue-normalization cclcgceli03.in2p3.fr:2119/jobmanager-bqs-long
  cclcgceli03.in2p3.fr:2119/jobmanager-bqs-long 2500.0
