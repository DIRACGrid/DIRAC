.. _dirac-wms-job-get-output:

========================
dirac-wms-job-get-output
========================

Retrieve output sandbox for a DIRAC job

Usage::

  dirac-wms-job-get-output [option|cfgfile] ... JobID ...

Arguments::

  JobID:    DIRAC Job ID or a name of the file with JobID per line

Options::

  -D  --Dir <value>            : Store the output in this directory
  -f  --File <value>           : Get output for jobs with IDs from the file
  -g  --JobGroup <value>       : Get output for jobs in the given group

Example::

  $ dirac-wms-job-get-output 1
  Job output sandbox retrieved in 1/
