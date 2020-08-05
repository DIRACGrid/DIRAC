.. _admin_dirac-admin-add-group:

=====================
dirac-admin-add-group
=====================

Add or Modify a Group info in DIRAC

Usage::

  dirac-admin-add-group [option|cfgfile] ... Property=<Value> ...

Arguments::

  Property=<Value>: Other properties to be added to the User like (VOMSRole=XXXX)

Options::

  -G  --GroupName <value>      : Name of the Group (Mandatory)
  -U  --UserName <value>       : Short Name of user to be added to the Group (Allow Multiple instances or None)
  -P  --Property <value>       : Property to be added to the Group (Allow Multiple instances or None)

Example::

  $ dirac-admin-add-group -G dirac_test
  $
