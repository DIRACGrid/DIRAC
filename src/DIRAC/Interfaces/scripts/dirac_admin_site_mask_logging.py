#!/usr/bin/env python
########################################################################
# File :    dirac-admin-site-mask-logging
# Author :  Stuart Paterson
########################################################################
"""
Retrieves site mask logging information.

Usage:
  dirac-admin-site-mask-logging [options] ... Site ...

Arguments:
  Site:     Name of the Site

Example:
  $ dirac-admin-site-mask-logging LCG.IN2P3.fr
  Site Mask Logging Info for LCG.IN2P3.fr
  Active  2010-12-08 21:28:16 ( atsareg )
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
    result = diracAdmin.getSiteMaskLogging(site, printOutput=True)
    if not result['OK']:
      errorList.append((site, result['Message']))
      exitCode = 2

  for error in errorList:
    print("ERROR %s: %s" % error)

  DIRAC.exit(exitCode)


if __name__ == "__main__":
  main()
