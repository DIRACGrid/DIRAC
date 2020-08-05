.. _admin_dirac-configuration-dump-local-cache:

====================================
dirac-configuration-dump-local-cache
====================================

Dump DIRAC Configuration data

Usage::

  dirac-configuration-dump-local-cache [option|cfgfile] ...

Options::

  -f  --file <value>           : Dump Configuration data into <file>
  -r  --raw                    : Do not make any modification to the data

Example::

  $ dirac-configuration-dump-local-cache -f /tmp/dump-conf.txt
