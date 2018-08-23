#!/usr/bin/env python

"""
  Get production transformations
"""

__RCSID__ = "$Id$"

import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.PrettyPrint import printTable

Script.setUsageMessage('\n'.join([__doc__.split('\n')[1],
                                  'Usage:',
                                  '  %s prodID' % Script.scriptName,
                                  'Arguments:',
                                  '  prodID: Production ID',
                                  '\ne.g: %s 381' % Script.scriptName,
                                  ]))


Script.parseCommandLine()

from DIRAC.ProductionSystem.Client.ProductionClient import ProductionClient

args = Script.getPositionalArgs()
if (len(args) != 1):
  Script.showHelp()

# get arguments
prodID = args[0]

prodClient = ProductionClient()
res = prodClient.getProductionTransformations(prodID)

fields = ['ProductionID', 'TransformationID', 'ParentTransformationID', 'LastUpdate', 'InsertedTime']
records = []

if res['OK']:
  transList = res['Value']
  for trans in transList:
    records.append([str(trans['ProductionID']), str(trans['TransformationID']), str(trans['ParentTransformationID']),
                    str(trans['LastUpdate']), str(trans['InsertedTime'])])
else:
  DIRAC.gLogger.error(res['Message'])
  DIRAC.exit(-1)

printTable(fields, records)

DIRAC.exit(0)
