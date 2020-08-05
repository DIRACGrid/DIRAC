.. _admin_dirac-admin-users-with-proxy:

============================
dirac-admin-users-with-proxy
============================

Usage::

  dirac-admin-users-with-proxy.py (<options>|<cfgFile>)*

Options::

  -v  --valid <value>          : Required HH:MM for the users

Example::

  $ dirac-admin-users-with-proxy
  * vhamar
  DN         : /O=GRID-FR/C=FR/O=CNRS/OU=CPPM/CN=Vanessa Hamar
  group      : dirac_admin
  not after  : 2011-06-29 12:04:25
  persistent : False
  -
  DN         : /O=GRID-FR/C=FR/O=CNRS/OU=CPPM/CN=Vanessa Hamar
  group      : dirac_pilot
  not after  : 2011-06-29 12:04:27
  persistent : False
  -
  DN         : /O=GRID-FR/C=FR/O=CNRS/OU=CPPM/CN=Vanessa Hamar
  group      : dirac_user
  not after  : 2011-06-29 12:04:30
  persistent : True
