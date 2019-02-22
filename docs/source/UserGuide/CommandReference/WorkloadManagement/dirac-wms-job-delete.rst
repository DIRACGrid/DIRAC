.. _dirac-wms-job-delete:

====================
dirac-wms-job-delete
====================

Delete DIRAC job from WMS, if running it will be killed

Usage::

 dirac-wms-job-delete [option|cfgfile] ... JobID ...

Arguments::

 JobID: DIRAC Job ID

Options::

  -f  --File <value>           : Get output for jobs with IDs from the file
  -g  --JobGroup <value>       : Get output for jobs in the given group

Example::
 

  $ dirac-wms-job-delete 12
  Deleted job 12
