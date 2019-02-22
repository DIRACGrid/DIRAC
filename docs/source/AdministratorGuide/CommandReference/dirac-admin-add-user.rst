.. _admin_dirac-admin-add-user:

====================
dirac-admin-add-user
====================

Add or Modify a User info in DIRAC

Usage::

  dirac-admin-add-user [option|cfgfile] ... Property=<Value> ...

Arguments::

 Property=<Value>: Properties to be added to the User like (Phone=XXXX)

Options::

  -N  --UserName <value>       : Short Name of the User (Mandatory)
  -D  --UserDN <value>         : DN of the User Certificate (Mandatory)
  -M  --UserMail <value>       : eMail of the user (Mandatory)
  -G  --UserGroup <value>      : Name of the Group for the User (Allow Multiple instances or None)

Example::

  $ dirac-admin-add-user -N vhamar -D /O=GRID-FR/C=FR/O=CNRS/OU=CPPM/CN=Vanessa Hamar -M hamar@cppm.in2p3.fr -G dirac_user
  $
