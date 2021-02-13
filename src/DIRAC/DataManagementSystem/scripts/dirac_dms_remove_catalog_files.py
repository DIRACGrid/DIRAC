#!/usr/bin/env python
########################################################################
# $Header:  $
########################################################################
"""
Remove the given file or a list of files from the File Catalog

Usage:
  dirac-file-remove-catalog-files <LFN | fileContainingLFNs>

Example:
  $ dirac-dms-remove-catalog-files   /formation/user/v/vhamar/1/1134/StdOut
  Successfully removed 1 catalog files.
"""
from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

__RCSID__ = "$Id$"

from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.DIRACScript import DIRACScript
from DIRAC import exit as dexit
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

  from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
  fc = FileCatalog()
  import os

  args = Script.getPositionalArgs()

  if len(args) < 1:
    Script.showHelp(exitCode=1)
  else:
    inputFileName = args[0]

  if os.path.exists(inputFileName):
    inputFile = open(inputFileName, 'r')
    string = inputFile.read()
    lfns = [lfn.strip() for lfn in string.splitlines()]
    inputFile.close()
  else:
    lfns = [inputFileName]

  res = fc.removeFile(lfns)
  if not res['OK']:
    print("Error:", res['Message'])
    dexit(1)
  for lfn in sorted(res['Value']['Failed'].keys()):
    message = res['Value']['Failed'][lfn]
    print('Error: failed to remove %s: %s' % (lfn, message))
  print('Successfully removed %d catalog files.' % (len(res['Value']['Successful'])))


if __name__ == "__main__":
  main()
