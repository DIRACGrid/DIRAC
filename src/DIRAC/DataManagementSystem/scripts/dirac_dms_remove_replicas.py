#!/usr/bin/env python
"""
Remove the given file replica or a list of file replicas from the File Catalog
and from the storage.

Usage:
   dirac-dms-remove-replicas <LFN | fileContainingLFNs> SE [SE]

Example:
  $ dirac-dms-remove-replicas /formation/user/v/vhamar/Test.txt IBCP-disk
  Successfully removed DIRAC-USER replica of /formation/user/v/vhamar/Test.txt
"""
from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

__RCSID__ = "$Id$"

from DIRAC import exit as DIRACExit
from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.DIRACScript import DIRACScript


@DIRACScript()
def main():
  Script.parseCommandLine()

  from DIRAC.Core.Utilities.List import breakListIntoChunks
  from DIRAC.DataManagementSystem.Client.DataManager import DataManager
  dm = DataManager()
  import os
  inputFileName = ""
  storageElementNames = []
  args = Script.getPositionalArgs()

  if len(args) < 2:
    Script.showHelp(exitCode=1)
  else:
    inputFileName = args[0]
    storageElementNames = args[1:]

  if os.path.exists(inputFileName):
    inputFile = open(inputFileName, 'r')
    string = inputFile.read()
    lfns = [lfn.strip() for lfn in string.splitlines()]
    inputFile.close()
  else:
    lfns = [inputFileName]
  for lfnList in breakListIntoChunks(sorted(lfns, reverse=True), 500):
    for storageElementName in storageElementNames:
      res = dm.removeReplica(storageElementName, lfnList)
      if not res['OK']:
        print('Error:', res['Message'])
        continue
      for lfn in sorted(res['Value']['Successful']):
        print('Successfully removed %s replica of %s' % (storageElementName, lfn))
      for lfn in sorted(res['Value']['Failed']):
        message = res['Value']['Failed'][lfn]
        print('Error: failed to remove %s replica of %s: %s' % (storageElementName, lfn, message))


if __name__ == "__main__":
  main()
