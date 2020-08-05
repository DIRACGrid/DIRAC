.. _admin_dirac-admin-ban-site:

====================
dirac-admin-ban-site
====================

Remove Site from Active mask for current Setup

Usage::

  dirac-admin-ban-site [option|cfgfile] ... Site Comment

Arguments::

  Site:     Name of the Site
  Comment:  Reason of the action

Options::

  -E  --email <value>          : Boolean True/False (True by default)

Example::

  $ dirac-admin-ban-site LCG.IN2P3.fr 'Pilot installation problems'
