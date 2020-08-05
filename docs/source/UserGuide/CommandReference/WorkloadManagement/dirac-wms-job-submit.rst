.. _dirac-wms-job-submit:

====================
dirac-wms-job-submit
====================

Submit jobs to DIRAC WMS

Usage::

  dirac-wms-job-submit [option|cfgfile] ... JDL ...

Arguments::

  JDL:      Path to JDL file

Options::

  -f  --File <value>           : Writes job ids to file <value>
  -r  --UseJobRepo <value>     : Use the job repository

Example::

  $ dirac-wms-job-submit Simple.jdl
  JobID = 11
