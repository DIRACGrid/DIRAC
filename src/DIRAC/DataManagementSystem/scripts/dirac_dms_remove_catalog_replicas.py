#!/usr/bin/env python
"""
Remove the given file replica or a list of file replicas from the File Catalog
This script should be used with great care as it may leave dark data in the storage!
Use dirac-dms-remove-replicas instead

Usage:
  dirac-dms-remove-catalog-replicas <LFN | fileContainingLFNs> <SE>
"""
from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

__RCSID__ = "$Id$"

import os

from DIRAC import exit as dexit
from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.DIRACScript import DIRACScript
from DIRAC import gLogger


@DIRACScript()
def main():
  Script.parseCommandLine()

  from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
  allowUsers = Operations().getValue("DataManagement/AllowUserReplicaManagement", False)

  from DIRAC.Core.Security.ProxyInfo import getProxyInfo
  res = getProxyInfo()
  if not res['OK']:
    gLogger.fatal("Can't get proxy info", res['Message'])
    dexit(1)
  properties = res['Value'].get('groupProperties', [])

  if not allowUsers:
    if 'FileCatalogManagement' not in properties:
      gLogger.error("You need to use a proxy from a group with FileCatalogManagement")
      dexit(5)

  from DIRAC.DataManagementSystem.Client.DataManager import DataManager
  dm = DataManager()
  args = Script.getPositionalArgs()
  if len(args) < 2:
    Script.showHelp(exitCode=1)
  else:
    inputFileName = args[0]
    storageElementName = args[1]

  if os.path.exists(inputFileName):
    inputFile = open(inputFileName, 'r')
    string = inputFile.read()
    lfns = [lfn.strip() for lfn in string.splitlines()]
    inputFile.close()
  else:
    lfns = [inputFileName]

  res = dm.removeReplicaFromCatalog(storageElementName, lfns)
  if not res['OK']:
    print(res['Message'])
    dexit(0)
  for lfn in sorted(res['Value']['Failed']):
    message = res['Value']['Failed'][lfn]
    print('Failed to remove %s replica of %s: %s' % (storageElementName, lfn, message))
  print('Successfully remove %d catalog replicas at %s' % (len(res['Value']['Successful']), storageElementName))


if __name__ == "__main__":
  main()
