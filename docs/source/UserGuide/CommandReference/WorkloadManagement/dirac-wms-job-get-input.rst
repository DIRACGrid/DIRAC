.. _dirac-wms-job-get-input:

=======================
dirac-wms-job-get-input
=======================

Retrieve input sandbox for DIRAC Job

Usage::

  dirac-wms-job-get-input [option|cfgfile] ... JobID ...

Arguments::

  JobID:    DIRAC Job ID

Options::

  -D  --Dir <value>            : Store the output in this directory

Example::

  $ dirac-wms-job-get-input 13
  Job input sandbox retrieved in InputSandbox13/
