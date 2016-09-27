===============================
dirac-wms-job-get-output
===============================

  Retrieve output sandbox for a DIRAC job

Usage::

  dirac-wms-job-get-output [option|cfgfile] ... JobID ...

Arguments::

  JobID:    DIRAC Job ID or a name of the file with JobID per line 

 

Options::

  -D:  --Dir=            : Store the output in this directory 

Example::

  $ dirac-wms-job-get-output 1
  Job output sandbox retrieved in 1/


