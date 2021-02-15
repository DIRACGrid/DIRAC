#! /usr/bin/env python
########################################################################
# File :    dirac-admin-kill-pilot
# Author :  A.T.
########################################################################
"""
Kill the specified pilot

Usage:
  dirac-admin-kill-pilot <pilot reference>
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
__RCSID__ = "$Id$"

import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.DIRACScript import DIRACScript


@DIRACScript()
def main():
  Script.parseCommandLine(ignoreErrors=True)
  args = Script.getPositionalArgs()

  if len(args) < 1:
    Script.showHelp(exitCode=1)

  pilotRef = args[0]

  from DIRAC.Interfaces.API.DiracAdmin import DiracAdmin
  diracAdmin = DiracAdmin()
  exitCode = 0

  result = diracAdmin.killPilot(pilotRef)
  if not result['OK']:
    DIRAC.gLogger.error('Failed to kill pilot', pilotRef)
    DIRAC.gLogger.error(result['Message'])
    exitCode = 1

  DIRAC.exit(exitCode)


if __name__ == "__main__":
  main()
