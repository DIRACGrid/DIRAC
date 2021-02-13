#!/usr/bin/env python
"""
Get summary informations of all productions

Usage:
  dirac-prod-get-all
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

  prodClient = ProductionClient()
  res = prodClient.getProductions()

  fields = [
      'ProductionName',
      'Status',
      'ProductionID',
      'CreationDate',
      'LastUpdate',
      'AuthorDN',
      'AuthorGroup']
  records = []

  if res['OK']:
    prodList = res['Value']
    if not isinstance(res['Value'], list):
      prodList = [res['Value']]
    for prod in prodList:
      records.append(
          [
              str(
                  prod['ProductionName']), str(
                  prod['Status']), str(
                  prod['ProductionID']), str(
                  prod['CreationDate']), str(
                  prod['LastUpdate']), str(
                  prod['AuthorDN']), str(
                  prod['AuthorGroup'])])
  else:
    DIRAC.gLogger.error(res['Message'])
    DIRAC.exit(-1)

  printTable(fields, records)

  DIRAC.exit(0)


if __name__ == "__main__":
  main()
