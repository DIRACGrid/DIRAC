#!/usr/bin/env python
########################################################################
# File :    dirac-admin-get-banned-sites
# Author :  Stuart Paterson
########################################################################
"""
Usage:
  dirac-admin-get-banned-sites [options] ...

Example:
  $ dirac-admin-get-banned-sites
  LCG.IN2P3.fr                      Site not present in logging table
"""
from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

__RCSID__ = "$Id$"

from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.DIRACScript import DIRACScript


@DIRACScript()
def main():
  Script.parseCommandLine(ignoreErrors=True)

  from DIRAC import gLogger, exit as DIRACExit
  from DIRAC.Interfaces.API.DiracAdmin import DiracAdmin

  diracAdmin = DiracAdmin()

  result = diracAdmin.getBannedSites()
  if result['OK']:
    bannedSites = result['Value']
  else:
    gLogger.error(result['Message'])
    DIRACExit(2)

  for site in bannedSites:
    result = diracAdmin.getSiteMaskLogging(site, printOutput=True)
    if not result['OK']:
      gLogger.error(result['Message'])
      DIRACExit(2)

  DIRACExit(0)


if __name__ == "__main__":
  main()
