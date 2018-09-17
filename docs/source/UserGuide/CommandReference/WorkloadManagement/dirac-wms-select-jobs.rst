============================
dirac-wms-select-jobs
============================

  Select DIRAC jobs matching the given conditions

Usage::

  dirac-wms-select-jobs [option|cfgfile] ... JobID ... 

 

Options::

  -    --Status=         : Primary status 

  -    --MinorStatus=    : Secondary status 

  -    --ApplicationStatus= : Application status 

  -    --Site=           : Execution site 

  -    --Owner=          : Owner (DIRAC nickname) 

  -    --JobGroup=       : Select jobs for specified job group(s)

  -    --Date=           : Date in YYYY-MM-DD format, if not specified default is today 

  -    --Maximum=        : Maximum number of jobs shown (default or 0 means all)
