.. _dirac-resource-info:

===================
dirac-resource-info
===================

Get information on configured resources

Usage::

  dirac-resource-info [option|cfgfile]

Options::

  -C  --ce                     : Get CE info
  -S  --se                     : Get SE info
  -V  --vo <value>             : Get resources for the given VO

Example::

  $ dirac-resource-info --vo enmr.eu --ce
        Site             CE                               CEType      Queue                        Status
  ===========================================================================================================
     1  EGI.LSGAMC.nl    gb-ce-amc.amc.nl                 CREAM       cream-pbs-medium             Active
     2                                                                cream-pbs-express            Active
     3                                                                cream-pbs-long               Active
     4  EGI.UJ.za        glite-ce.grid.uj.ac.za           CREAM       cream-pbs-sagrid             InActive
     5  EGI.GARR.it      gridsrv2-4.dir.garr.it           CREAM       cream-pbs-enmr               Active

