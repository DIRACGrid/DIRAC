.. _admin_dirac-admin-add-host:

====================
dirac-admin-add-host
====================

Add or Modify a Host info in DIRAC

Usage::

  dirac-admin-add-host [option|cfgfile] ... Property=<Value> ...

Arguments::

  Property=<Value>: Other properties to be added to the User like (Responsible=XXXX)

Options::

  -H  --HostName <value>       : Name of the Host (Mandatory)
  -D  --HostDN <value>         : DN of the Host Certificate (Mandatory)
  -P  --Property <value>       : Property to be added to the Host (Allow Multiple instances or None)

Example::

  $ dirac-admin-add-host -H dirac.i2np3.fr -D /O=GRID-FR/C=FR/O=CNRS/OU=CC-IN2P3/CN=dirac.in2p3.fr
