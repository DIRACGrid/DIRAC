#!/usr/bin/env python

"""
  Get the description of a given production
"""

from __future__ import print_function
from __future__ import absolute_import
from __future__ import division
__RCSID__ = "$Id$"

import DIRAC
from DIRAC.Core.Base import Script

Script.setUsageMessage('\n'.join([__doc__.split('\n')[1],
                                  'Usage:',
                                  '  %s prodID' % Script.scriptName,
                                  'Arguments:',
                                  '  prodID: Production ID'
                                  ]))


Script.parseCommandLine()

from DIRAC.ProductionSystem.Client.ProductionClient import ProductionClient

prodClient = ProductionClient()

# get arguments
args = Script.getPositionalArgs()
if (len(args) != 1):
  Script.showHelp()
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
