======================
dirac-dms-show-ftsjobs
======================

display information about FTSJobs for a given requestID
Usage::

  dirac-dms-show-ftsjobs [option|cfgfile] requestID

Argument::

  requestID: RequestDB.Request.RequestID

General options::

  -o  --option <value>         : Option=value to add
  -s  --section <value>        : Set base section for relative parsed options
  -c  --cert <value>           : Use server certificate to connect to Core Services
  -d  --debug                  : Set debug mode (-ddd is extra debug)
  -   --autoreload             : Automatically restart if there's any change in the module
  -   --license                : Show DIRAC's LICENSE
  -h  --help                   : Shows this help
