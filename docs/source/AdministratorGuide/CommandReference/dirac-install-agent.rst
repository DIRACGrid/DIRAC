==========================
dirac-install-agent
==========================

2013-02-06 12:30:28 UTC Framework NOTICE: DIRAC Root Path = /afs/in2p3.fr/home/h/hamar/DIRAC-v6r7

Do the initial installation and configuration of a DIRAC agent

Usage::

  dirac-install-agent [option|cfgfile] ... System Agent|System/Agent

Arguments::

  System:  Name of the DIRAC system (ie: WorkloadManagement)

  Agent:   Name of the DIRAC agent (ie: JobCleaningAgent) 

 

Options::

  -w   --overwrite       : Overwrite the configuration in the global CS 

  -m:  --module=         : Python module name for the agent code 

  -p:  --parameter=      : Special agent option  


