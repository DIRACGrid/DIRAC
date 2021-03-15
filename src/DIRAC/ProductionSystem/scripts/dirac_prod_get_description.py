#!/usr/bin/env python
"""
Get the description of a given production

Usage:
  dirac-prod-get-description prodID

Arguments:
  prodID: Production ID (mandatory)

Example:
  $ dirac-prod-get-description 381
"""

from __future__ import print_function
from __future__ import absolute_import
from __future__ import division
__RCSID__ = "$Id$"

import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.DIRACScript import DIRACScript


@DIRACScript()
def main(self):  # pylint: disable=no-value-for-parameter
  self.parseCommandLine()

  from DIRAC.ProductionSystem.Client.ProductionClient import ProductionClient

  prodClient = ProductionClient()

  # get arguments
  args = self.getPositionalArgs()
  if len(args) < 1:
    self.showHelp(exitCode=1)
  else:
    prodID = args[0]
    res = prodClient.getProduction(prodID)

  if res['OK']:
    prod = res['Value']
  else:
    DIRAC.gLogger.error(res['Message'])
    DIRAC.exit(-1)

  print('Description for production %s:\n' % prodID)
  print(prod['Description'])

  DIRAC.exit(0)


if __name__ == "__main__":
  main()
