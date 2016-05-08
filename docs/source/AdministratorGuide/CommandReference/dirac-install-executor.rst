=============================
dirac-install-executor
=============================

Install an executor.

Usage::

  dirac-install-executor [option|cfgfile] ... System Executor|System/Executor

Arguments::

  System:  Name of the DIRAC system (ie: WorkloadManagement)

  Service: Name of the DIRAC executor (ie: JobPath) 

 

Options::

  -w   --overwrite       : Overwrite the configuration in the global CS 

  -m:  --module=         : Python module name for the executor code 

  -p:  --parameter=      : Special executor option  


