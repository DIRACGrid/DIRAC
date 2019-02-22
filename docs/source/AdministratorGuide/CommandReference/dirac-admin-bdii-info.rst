.. _admin_dirac-admin-bdii-info:

=====================
dirac-admin-bdii-info
=====================

Check info on BDII for a given CE or site

Usage::

  dirac-admin-bdii-info [option|cfgfile] ... <info> <Site|CE>

Arguments::

  Site:     Name of the Site (i.e. CERN-PROD)
  CE:       Name of the CE (i.e. cccreamceli05.in2p3.fr)
  info:     Accepted values (ce|ce-state|ce-cluster|ce-vo|site|site-se)

Options::

  -H  --host <value>           : BDII host
  -V  --vo <value>             : vo
