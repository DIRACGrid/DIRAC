.. _admin_dirac-admin-list-users:

======================
dirac-admin-list-users
======================

Lists the users in the Configuration. If no group is specified return all users.

Usage::

  dirac-admin-list-users [option|cfgfile] ... [Group] ...

Arguments::

  Group:    Only users from this group (default: all)

Options::

  -e  --extended               : Show extended info

Example::

  $ dirac-admin-list-users
  All users registered:
  vhamar
  msapunov
  atsareg
