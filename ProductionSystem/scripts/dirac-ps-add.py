#!/usr/bin/env python

"""
  Create a production
"""

__RCSID__ = "$Id$"

import DIRAC
from DIRAC.Core.Base import Script

Script.setUsageMessage('\n'.join([__doc__.split('\n')[1],
                                  'Usage:',
                                  '  %s prodName' % Script.scriptName,
                                  'Arguments:',
                                  '  prodName: Production Name'
                                  ]))


Script.parseCommandLine()

from DIRAC.ProductionSystem.Client.ProductionClient import ProductionClient

prodClient = ProductionClient()

# get arguments
args = Script.getPositionalArgs()
if (len(args) != 1):
  Script.showHelp()

prodName = args[0]

# Create a production
res = prodClient.addProduction(prodName)

if not res['OK']:
  DIRAC.gLogger.error(res['Message'])
  DIRAC.exit(-1)

prodID = res['Value']

DIRAC.gLogger.notice('Production %s successfully created' % prodID)

DIRAC.exit(0)
