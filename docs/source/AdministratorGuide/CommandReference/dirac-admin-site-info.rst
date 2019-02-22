.. _admin_dirac-admin-site-info:

=====================
dirac-admin-site-info
=====================

Print Configuration information for a given Site

Usage::

  dirac-admin-site-info [option|cfgfile] ... Site ...

Arguments::

  Site:     Name of the Site

Example::

  $ dirac-admin-site-info LCG.IN2P3.fr
  {'CE': 'cclcgceli01.in2p3.fr, cclcgceli03.in2p3.fr, sbgce1.in2p3.fr, clrlcgce01.in2p3.fr, clrlcgce02.in2p3.fr, clrlcgce03.in2p3.fr, grid10.lal.in2p3.fr, polgrid1.in2p3.fr',
   'Coordinates': '4.8655:45.7825',
   'Mail': 'grid.admin@cc.in2p3.fr',
   'MoUTierLevel': '1',
   'Name': 'IN2P3-CC',
   'SE': 'IN2P3-disk, DIRAC-USER'}
