.. _admin_dirac-admin-sync-users-from-file:

================================
dirac-admin-sync-users-from-file
================================

Sync users in Configuration with the cfg contents.

Usage::

  dirac-admin-sync-users-from-file [option|cfgfile] ... UserCfg

Arguments::

  UserCfg:  Cfg FileName with Users as sections containing DN, Groups, and other properties as options

Options::

  -t  --test                   : Only test. Don't commit changes

Example::

  $ dirac-admin-sync-users-from-file file_users.cfg
