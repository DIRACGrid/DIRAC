.. _admin_dirac-admin-allow-site:

======================
dirac-admin-allow-site
======================

Add Site to Active mask for current Setup

Usage::

  dirac-admin-allow-site [option|cfgfile] ... Site Comment

Arguments::

  Site:     Name of the Site
  Comment:  Reason of the action

Options::

  -E  --email <value>          : Boolean True/False (True by default)

Example::
  

  $ dirac-admin-allow-site LCG.IN2P3.fr 'FRANCE'
