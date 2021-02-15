#!/usr/bin/env python
########################################################################
# File :    dirac-admin-site-info
# Author :  Stuart Paterson
########################################################################
"""
Print Configuration information for a given Site

Usage:
  dirac-admin-site-info [options] ... Site ...

Arguments:
  Site:     Name of the Site

Example:
  $ dirac-admin-site-info LCG.IN2P3.fr
  {'CE': 'cclcgceli01.in2p3.fr, cclcgceli03.in2p3.fr, sbgce1.in2p3.fr',
   'Coordinates': '4.8655:45.7825',
   'Mail': 'grid.admin@cc.in2p3.fr',
   'MoUTierLevel': '1',
   'Name': 'IN2P3-CC',
   'SE': 'IN2P3-disk, DIRAC-USER'}
"""
from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

__RCSID__ = "$Id$"

import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.DIRACScript import DIRACScript


@DIRACScript()
def main():
  Script.parseCommandLine(ignoreErrors=True)
  args = Script.getPositionalArgs()

  if len(args) < 1:
    Script.showHelp()

  from DIRAC.Interfaces.API.DiracAdmin import DiracAdmin
  diracAdmin = DiracAdmin()
  exitCode = 0
  errorList = []

  for site in args:

    result = diracAdmin.getSiteSection(site, printOutput=True)
    if not result['OK']:
      errorList.append((site, result['Message']))
      exitCode = 2

  for error in errorList:
    print("ERROR %s: %s" % error)

  DIRAC.exit(exitCode)


if __name__ == "__main__":
  main()
