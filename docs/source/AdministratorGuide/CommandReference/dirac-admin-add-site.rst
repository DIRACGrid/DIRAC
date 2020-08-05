.. _admin_dirac-admin-add-site:

====================
dirac-admin-add-site
====================

Add a new DIRAC SiteName to DIRAC Configuration, including one or more CEs.

Usage::

  dirac-admin-add-site [option|cfgfile] ... DIRACSiteName GridSiteName CE [CE] ...

Arguments::

  DIRACSiteName: Name of the site for DIRAC in the form GRID.LOCATION.COUNTRY (ie:LCG.CERN.ch)
  GridSiteName: Name of the site in the Grid (ie: CERN-PROD)
  CE: Name of the CE to be included in the site (ie: ce111.cern.ch)

Example::
  

  $ dirac-admin-add-site LCG.IN2P3.fr IN2P3-Site
