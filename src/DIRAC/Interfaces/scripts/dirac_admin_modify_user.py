#!/usr/bin/env python
########################################################################
# File :    dirac-admin-modify-user
# Author :  Adrian Casajus
########################################################################
"""
Modify a user in the CS.

Usage:
  dirac-admin-modify-user [options] ... user DN group [group] ...

Arguments:
  user:     User name (mandatory)
  DN:       DN of the User (mandatory)
  group:    Add the user to the group (mandatory)

Example:
  $ dirac-admin-modify-user vhamar /C=FR/O=Org/CN=User dirac_user
"""
from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

__RCSID__ = "$Id$"

import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.DIRACScript import DIRACScript


@DIRACScript()
def main():
  Script.registerSwitch("p:", "property=", "Add property to the user <name>=<value>")
  Script.registerSwitch("f", "force", "create the user if it doesn't exist")
  Script.parseCommandLine(ignoreErrors=True)

  args = Script.getPositionalArgs()

  if len(args) < 3:
    Script.showHelp(exitCode=1)

  from DIRAC.Interfaces.API.DiracAdmin import DiracAdmin
  diracAdmin = DiracAdmin()
  exitCode = 0
  forceCreation = False
  errorList = []

  userProps = {}
  for unprocSw in Script.getUnprocessedSwitches():
    if unprocSw[0] in ("f", "force"):
      forceCreation = True
    elif unprocSw[0] in ("p", "property"):
      prop = unprocSw[1]
      pl = prop.split("=")
      if len(pl) < 2:
        errorList.append(("in arguments", "Property %s has to include a '=' to separate name from value" % prop))
        exitCode = 255
      else:
        pName = pl[0]
        pValue = "=".join(pl[1:])
        print("Setting property %s to %s" % (pName, pValue))
        userProps[pName] = pValue

  userName = args[0]
  userProps['DN'] = args[1]
  userProps['Groups'] = args[2:]

  if not diracAdmin.csModifyUser(userName, userProps, createIfNonExistant=forceCreation):
    errorList.append(("modify user", "Cannot modify user %s" % userName))
    exitCode = 255
  else:
    result = diracAdmin.csCommitChanges()
    if not result['OK']:
      errorList.append(("commit", result['Message']))
      exitCode = 255

  for error in errorList:
    print("ERROR %s: %s" % error)

  DIRAC.exit(exitCode)


if __name__ == "__main__":
  main()
