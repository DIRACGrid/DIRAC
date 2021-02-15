#!/usr/bin/env python
"""
Get the transformations belonging to a given production

Usage:
  dirac-prod-get-trans prodID

Arguments:
  prodID: Production ID (mandatory)

Example:
  $ dirac-prod-get-trans 381
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.PrettyPrint import printTable
from DIRAC.Core.Utilities.DIRACScript import DIRACScript


@DIRACScript()
def main():
  Script.parseCommandLine()

  from DIRAC.ProductionSystem.Client.ProductionClient import ProductionClient
  from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient

  args = Script.getPositionalArgs()
  if len(args) < 1:
    Script.showHelp(exitCode=1)

  # get arguments
  prodID = args[0]

  prodClient = ProductionClient()
  transClient = TransformationClient()

  res = prodClient.getProductionTransformations(prodID)
  transIDs = []

  if res['OK']:
    transList = res['Value']
    if not transList:
      DIRAC.gLogger.notice('No transformation associated with production %s' % prodID)
      DIRAC.exit(-1)
    for trans in transList:
      transIDs.append(trans['TransformationID'])
  else:
    DIRAC.gLogger.error(res['Message'])
    DIRAC.exit(-1)

  fields = [
      'TransformationName',
      'Status',
      'F_Proc.',
      'F_Proc.(%)',
      'TransformationID',
      'ProductionID',
      'Prod_LastUpdate',
      'Prod_InsertedTime']

  records = []

  paramShowNames = ['TransformationID', 'TransformationName', 'Type', 'Status', 'Files_Total', 'Files_PercentProcessed',
                    'Files_Processed', 'Files_Unused', 'Jobs_TotalCreated', 'Jobs_Waiting',
                    'Jobs_Running', 'Jobs_Done', 'Jobs_Failed', 'Jobs_Stalled']
  resList = []

  res = transClient.getTransformationSummaryWeb({'TransformationID': transIDs}, [], 0, len(transIDs))

  if not res['OK']:
    DIRAC.gLogger.error(res['Message'])
    DIRAC.exit(-1)

  if res['Value']['TotalRecords'] > 0:
    paramNames = res['Value']['ParameterNames']
    for paramValues in res['Value']['Records']:
      paramShowValues = map(lambda pname: paramValues[paramNames.index(pname)], paramShowNames)
      showDict = dict(zip(paramShowNames, paramShowValues))
      resList.append(showDict)

  for res in resList:
    files_Processed = res['Files_Processed']
    files_PercentProcessed = res['Files_PercentProcessed']
    status = res['Status']
    type = res['Type']
    transName = res['TransformationName']
    transID = res['TransformationID']
    records.append([transName, status, str(files_Processed), str(files_PercentProcessed), str(transID), str(prodID),
                    str(trans['LastUpdate']), str(trans['InsertedTime'])])

  printTable(fields, records)

  DIRAC.exit(0)


if __name__ == "__main__":
  main()
