#!/usr/bin/env python

"""
  Clean a given production
"""

__RCSID__ = "$Id$"

import DIRAC
from DIRAC.Core.Base import Script

Script.setUsageMessage('\n'.join([__doc__.split('\n')[1],
                                  'Usage:',
                                  '  %s prodID' % Script.scriptName,
                                  'Arguments:',
                                  '  prodID: Production ID (mandatory)'
                                  ]))


Script.parseCommandLine()

from DIRAC.ProductionSystem.Client.ProductionClient import ProductionClient

args = Script.getPositionalArgs()
if len(args) < 1:
  Script.showHelp(exitCode=1)

# get arguments
prodID = args[0]

res = ProductionClient().setProductionStatus(prodID, 'Cleaned')
if not res['OK']:
  DIRAC.gLogger.error(res['Message'])
  DIRAC.exit(1)

DIRAC.gLogger.notice('Production %s successully cleaned' % prodID)
DIRAC.exit(0)
