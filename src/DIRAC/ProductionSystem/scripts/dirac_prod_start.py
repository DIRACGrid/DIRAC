#!/usr/bin/env python
"""
Start a given production

Usage:
  dirac-prod-start prodID

Arguments:
  prodID: Production ID (mandatory)

Example:
  $ dirac-prod-start 381
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
  Script.parseCommandLine()

  from DIRAC.ProductionSystem.Client.ProductionClient import ProductionClient

  args = Script.getPositionalArgs()
  if len(args) < 1:
    Script.showHelp(exitCode=1)

  # get arguments
  prodID = args[0]

  prodClient = ProductionClient()

  res = prodClient.setProductionStatus(prodID, 'Active')
  if res['OK']:
    DIRAC.gLogger.notice('Production %s successully started' % prodID)
  else:
    DIRAC.gLogger.error(res['Message'])
    DIRAC.exit(-1)

  DIRAC.exit(0)


if __name__ == "__main__":
  main()
