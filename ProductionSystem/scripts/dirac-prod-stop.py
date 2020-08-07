#!/usr/bin/env python

"""
  Stop a given production
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

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

prodClient = ProductionClient()

res = prodClient.setProductionStatus(prodID, 'Stopped')
if res['OK']:
  DIRAC.gLogger.notice('Production %s successully stopped' % prodID)
else:
  DIRAC.gLogger.error(res['Message'])
  DIRAC.exit(-1)

DIRAC.exit(0)
