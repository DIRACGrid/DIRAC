==========================================
dirac-wms-jobs-select-output-search
==========================================

  Retrieve output sandbox for DIRAC Jobs for the given selection and search for a string in their std.out

Usage::

  dirac-wms-jobs-select-output-search [option|cfgfile] ... String ...

Arguments::

  String:   string to search for 

 

Options::

  -    --Status=         : Primary status 

  -    --MinorStatus=    : Secondary status 

  -    --ApplicationStatus= : Application status 

  -    --Site=           : Execution site 

  -    --Owner=          : Owner (DIRAC nickname) 

  -    --JobGroup=       : Select jobs for specified job group 

  -    --Date=           : Date in YYYY-MM-DD format, if not specified default is today 

  -    --File=           : File name,if not specified default is std.out  


