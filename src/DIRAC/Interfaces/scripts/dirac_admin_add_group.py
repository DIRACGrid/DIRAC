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
from DIRAC.Core.Utilities.DIRACScript import DIRACScript


class Params(object):

  def __init__(self, script):
    self.__script = script
    self.groupName = None
    self.groupProperties = []
    self.userNames = []

  def setGroupName(self, arg):
    if self.groupName or not arg:
      self.__script.showHelp(exitCode=1)
    self.groupName = arg

  def addUserName(self, arg):
    if not arg:
      self.__script.showHelp(exitCode=1)
    if arg not in self.userNames:
      self.userNames.append(arg)

  def addProperty(self, arg):
    if not arg:
      self.__script.showHelp(exitCode=1)
    if arg not in self.groupProperties:
      self.groupProperties.append(arg)


@DIRACScript()
def main(self):
  params = Params(self)
  self.registerSwitch('G:', 'GroupName:', 'Name of the Group (Mandatory)', params.setGroupName)
  self.registerSwitch(
      'U:',
      'UserName:',
      'Short Name of user to be added to the Group (Allow Multiple instances or None)',
      params.addUserName)
  self.registerSwitch(
      'P:',
      'Property:',
      'Property to be added to the Group (Allow Multiple instances or None)',
      params.addProperty)

  self.parseCommandLine(ignoreErrors=True)

  if params.groupName is None:
    self.showHelp(exitCode=1)

  args = self.getPositionalArgs()

  from DIRAC.Interfaces.API.DiracAdmin import DiracAdmin
  diracAdmin = DiracAdmin()
  exitCode = 0
  errorList = []

  groupProps = {}
  if params.userNames:
    groupProps['Users'] = ', '.join(params.userNames)
  if params.groupProperties:
    groupProps['Properties'] = ', '.join(params.groupProperties)

  for prop in args:
    pl = prop.split("=")
    if len(pl) < 2:
      errorList.append(("in arguments", "Property %s has to include a '=' to separate name from value" % prop))
      exitCode = 255
    else:
      pName = pl[0]
      pValue = "=".join(pl[1:])
      self.gLogger.info("Setting property %s to %s" % (pName, pValue))
      groupProps[pName] = pValue

  if not diracAdmin.csModifyGroup(params.groupName, groupProps, createIfNonExistant=True)['OK']:
    errorList.append(("add group", "Cannot register group %s" % params.groupName))
    exitCode = 255
  else:
    result = diracAdmin.csCommitChanges()
    if not result['OK']:
      errorList.append(("commit", result['Message']))
      exitCode = 255

  for error in errorList:
    self.gLogger.error("%s: %s" % error)

  DIRAC.exit(exitCode)


if __name__ == "__main__":
  main()  # pylint: disable=no-value-for-parameter
