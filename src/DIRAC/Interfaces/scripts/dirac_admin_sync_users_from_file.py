#!/usr/bin/env python
########################################################################
# File :    dirac-admin-sync-users-from-file
# Author :  Adrian Casajus
########################################################################
"""
Sync users in Configuration with the cfg contents.

Usage:
  dirac-admin-sync-users-from-file [options] ... UserCfg

Arguments:
  UserCfg:  Cfg FileName with Users as sections containing DN, Groups, and other properties as options

Example:
  $ dirac-admin-sync-users-from-file file_users.cfg
"""
from __future__ import print_function
from __future__ import absolute_import
from __future__ import division
from diraccfg import CFG

import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.DIRACScript import DIRACScript

__RCSID__ = "$Id$"


@DIRACScript()
def main():
  Script.registerSwitch("t", "test", "Only test. Don't commit changes")
  Script.parseCommandLine(ignoreErrors=True)

  args = Script.getExtraCLICFGFiles()

  if len(args) < 1:
    Script.showHelp()

  from DIRAC.Interfaces.API.DiracAdmin import DiracAdmin
  diracAdmin = DiracAdmin()
  exitCode = 0
  testOnly = False
  errorList = []

  for unprocSw in Script.getUnprocessedSwitches():
    if unprocSw[0] in ("t", "test"):
      testOnly = True

  try:
    usersCFG = CFG().loadFromFile(args[0])
  except Exception as e:
    errorList.append("file open", "Can't parse file %s: %s" % (args[0], str(e)))
    errorCode = 1
  else:
    if not diracAdmin.csSyncUsersWithCFG(usersCFG):
      errorList.append(("modify users", "Cannot sync with %s" % args[0]))
      exitCode = 255

  if not exitCode and not testOnly:
    result = diracAdmin.csCommitChanges()
    if not result['OK']:
      errorList.append(("commit", result['Message']))
      exitCode = 255

  for error in errorList:
    print("ERROR %s: %s" % error)

  DIRAC.exit(exitCode)


if __name__ == "__main__":
  main()
