#!/usr/bin/env python

__RCSID__ = "$Id$"

# pylint: disable=wrong-import-position

import DIRAC
from DIRAC.Core.Base import Script

groupName = None
groupProperties = []
userNames = []


def setGroupName(arg):
  global groupName
  if groupName or not arg:
    Script.showHelp(1)
  groupName = arg


def addUserName(arg):
  global userNames
  if not arg:
    Script.showHelp(1)
  if arg not in userNames:
    userNames.append(arg)


def addProperty(arg):
  global groupProperties
  if not arg:
    Script.showHelp(1)
  if arg not in groupProperties:
    groupProperties.append(arg)


Script.setUsageMessage('\n'.join(['Add or Modify a Group info in DIRAC',
                                  '\nUsage:\n',
                                  '  %s [option|cfgfile] ... Property=<Value> ...' % Script.scriptName,
                                  '\nArguments:\n',
                                  '  Property=<Value>: Other properties to be added to the User like (VOMSRole=XXXX)',
                                  ]))

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
  Script.showHelp(1)

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
