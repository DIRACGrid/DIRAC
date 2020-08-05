.. _admin_dirac-admin-modify-user:

=======================
dirac-admin-modify-user
=======================

Modify a user in the CS.

Usage::

  dirac-admin-modify-user [option|cfgfile] ... user DN group [group] ...

Arguments::

  user:     User name
  DN:       DN of the User
  group:    Add the user to the group

Options::

  -p  --property <value>       : Add property to the user <name>=<value>
  -f  --force                  : create the user if it doesn't exist

Example::

  $ dirac-admin-modify-user vhamar group dirac_user
