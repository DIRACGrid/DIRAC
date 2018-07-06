==============================
dirac-dms-move-replica-request
==============================

Create a DIRAC MoveReplica request to be executed by the RMS

Usage::

  dirac-dms-move-replica-request [option|cfgfile] ... sourceSE LFN targetSE1 [targetSE2 ...]

Arguments::

  sourceSE:   source SE
  targetSE: target SE
  LFN:      LFN or file containing a List of LFNs

General options::

  -o  --option <value>         : Option=value to add
  -s  --section <value>        : Set base section for relative parsed options
  -c  --cert <value>           : Use server certificate to connect to Core Services
  -d  --debug                  : Set debug mode (-ddd is extra debug)
  -   --autoreload             : Automatically restart if there's any change in the module
  -   --license                : Show DIRAC's LICENSE
  -h  --help                   : Shows this help
