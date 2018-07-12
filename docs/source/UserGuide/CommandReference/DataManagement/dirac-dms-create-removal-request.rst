================================
dirac-dms-create-removal-request
================================

Create a DIRAC RemoveReplica|RemoveFile request to be executed by the RMS

Usage::

  dirac-dms-create-removal-request [option|cfgfile] ... SE LFN ...

Arguments::

  SE:       StorageElement|All
  LFN:      LFN or file containing a List of LFNs

General options::

  -o  --option <value>         : Option=value to add
  -s  --section <value>        : Set base section for relative parsed options
  -c  --cert <value>           : Use server certificate to connect to Core Services
  -d  --debug                  : Set debug mode (-ddd is extra debug)
  -   --autoreload             : Automatically restart if there's any change in the module
  -   --license                : Show DIRAC's LICENSE
  -h  --help                   : Shows this help
