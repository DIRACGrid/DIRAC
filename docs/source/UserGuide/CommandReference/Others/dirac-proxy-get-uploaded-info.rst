.. _dirac-proxy-get-uploaded-info:

=============================
dirac-proxy-get-uploaded-info
=============================

Usage::

  dirac-proxy-get-uploaded-info.py (<options>|<cfgFile>)*

Options::

  -u  --user <value>           : User to query (by default oneself)

Example::

  $ dirac-proxy-get-uploaded-info
  Checking for DNs /O=GRID-FR/C=FR/O=CNRS/OU=CPPM/CN=Vanessa Hamar
  --------------------------------------------------------------------------------------------------------
  | UserDN                                          | UserGroup   | ExpirationTime      | PersistentFlag |
  --------------------------------------------------------------------------------------------------------
  | /O=GRID-FR/C=FR/O=CNRS/OU=CPPM/CN=Vanessa Hamar | dirac_user  | 2011-06-29 12:04:25 | True           |
  --------------------------------------------------------------------------------------------------------
