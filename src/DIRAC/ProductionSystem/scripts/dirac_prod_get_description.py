#!/usr/bin/env python
"""
Get the description of a given production

Example:
  $ dirac-prod-get-description 381
"""
from __future__ import print_function
from __future__ import absolute_import
from __future__ import division
__RCSID__ = "$Id$"

import DIRAC
from DIRAC.Core.Utilities.DIRACScript import DIRACScript as Script


@Script()
def main():
  # Registering arguments will automatically add their description to the help menu
  Script.registerArgument("prodID: Production ID")
  _, args = Script.parseCommandLine()

  from DIRAC.ProductionSystem.Client.ProductionClient import ProductionClient

  prodClient = ProductionClient()

  # get arguments
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
