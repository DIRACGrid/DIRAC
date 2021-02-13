#!/usr/bin/env python
"""
Add or Modify a Group info in DIRAC

Usage:
  dirac-admin-add-group [options] ... Property=<Value> ...

Arguments:
  Property=<Value>: Other properties to be added to the Group like (VOMSRole=XXXX)

Example:
  $ dirac-admin-add-group -G dirac_test
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

# pylint: disable=wrong-import-position

import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.DIRACScript import DIRACScript

groupName = None
groupProperties = []
userNames = []


def setGroupName(arg):
  global groupName
  if groupName or not arg:
    Script.showHelp(exitCode=1)
  groupName = arg


def addUserName(arg):
  global userNames
  if not arg:
    Script.showHelp(exitCode=1)
  if arg not in userNames:
    userNames.append(arg)


def addProperty(arg):
  global groupProperties
  if not arg:
    Script.showHelp(exitCode=1)
  if arg not in groupProperties:
    groupProperties.append(arg)


@DIRACScript()
def main():
  global groupName
  global groupProperties
  global userNames
  Script.registerSwitch('G:', 'GroupName:', 'Name of the Group (Mandatory)', setGroupName)
  Script.registerSwitch(
      'U:',
      'UserName:',
      'Short Name of user to be added to the Group (Allow Multiple instances or None)',
      addUserName)
  Script.registerSwitch(
      'P:',
      'Property:',
      'Property to be added to the Group (Allow Multiple instances or None)',
      addProperty)

  Script.parseCommandLine(ignoreErrors=True)

  if groupName is None:
    Script.showHelp(exitCode=1)

  args = Script.getPositionalArgs()

  from DIRAC.Interfaces.API.DiracAdmin import DiracAdmin
  diracAdmin = DiracAdmin()
  exitCode = 0
  errorList = []

  groupProps = {}
  if userNames:
    groupProps['Users'] = ', '.join(userNames)
  if groupProperties:
    groupProps['Properties'] = ', '.join(groupProperties)

  for prop in args:
    pl = prop.split("=")
    if len(pl) < 2:
      errorList.append(("in arguments", "Property %s has to include a '=' to separate name from value" % prop))
      exitCode = 255
    else:
      pName = pl[0]
      pValue = "=".join(pl[1:])
      Script.gLogger.info("Setting property %s to %s" % (pName, pValue))
      groupProps[pName] = pValue

  if not diracAdmin.csModifyGroup(groupName, groupProps, createIfNonExistant=True)['OK']:
    errorList.append(("add group", "Cannot register group %s" % groupName))
    exitCode = 255
  else:
    result = diracAdmin.csCommitChanges()
    if not result['OK']:
      errorList.append(("commit", result['Message']))
      exitCode = 255

  for error in errorList:
    Script.gLogger.error("%s: %s" % error)

  DIRAC.exit(exitCode)


if __name__ == "__main__":
  main()
