#! /usr/bin/env python
"""
Clean the given directory or a list of directories by removing it and all the
contained files and subdirectories from the physical storage and from the
file catalogs.

Usage:
   dirac-dms-clean-directory <LFN_Directory | fileContainingLFN_Directories>

Example:
  $ dirac-dms-clean-directory /formation/user/v/vhamar/newDir
  Cleaning directory /formation/user/v/vhamar/newDir ...  OK
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import os

from DIRAC import exit as DIRACExit, gLogger
from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.DIRACScript import DIRACScript


@DIRACScript()
def main():
  Script.parseCommandLine()

  args = Script.getPositionalArgs()
  if len(args) != 1:
    Script.showHelp(exitCode=1)

  inputFileName = args[0]

  if os.path.exists(inputFileName):
    lfns = [lfn.strip().split()[0] for lfn in sorted(open(inputFileName, 'r').read().splitlines())]
  else:
    lfns = [inputFileName]

  from DIRAC.DataManagementSystem.Client.DataManager import DataManager
  dm = DataManager()
  retVal = 0
  for lfn in [lfn for lfn in lfns if lfn]:
    gLogger.notice("Cleaning directory %r ... " % lfn)
    result = dm.cleanLogicalDirectory(lfn)
    if not result['OK']:
      gLogger.error('Failed to clean directory', result['Message'])
      retVal = -1
    else:
      if not result['Value']['Failed']:
        gLogger.notice('OK')
      else:
        for folder, message in result['Value']['Failed'].items():
          gLogger.error('Failed to clean folder', "%r: %s" % (folder, message))
          retVal = -1

    DIRACExit(retVal)


if __name__ == "__main__":
  main()
