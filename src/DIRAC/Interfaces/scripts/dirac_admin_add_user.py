#!/usr/bin/env python
"""
Add or Modify a User info in DIRAC

Usage:
  dirac-admin-add-user [options] ... Property=<Value> ...

Arguments:
  Property=<Value>: Properties to be added to the User like (Phone=XXXX)

Example:
  $ dirac-admin-add-user -N vhamar -D /O=GRID/C=FR/O=CNRS/OU=CPPM/CN=Vanessa Hamar -M hamar@cppm.in2p3.fr -G dirac_user
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import DIRAC
from DIRAC.Core.Utilities.DIRACScript import DIRACScript


class Params(object):

  def __init__(self, script):
    self.__script = script
    self.userName = None
    self.userDN = None
    self.userMail = None
    self.userGroups = []

  def setUserName(self, arg):
    if self.userName or not arg:
      self.__script.showHelp(exitCode=1)
    self.userName = arg

  def setUserDN(self, arg):
    if self.userDN or not arg:
      self.__script.showHelp(exitCode=1)
    self.userDN = arg

  def setUserMail(self, arg):
    if self.userMail or not arg:
      self.__script.showHelp(exitCode=1)
    if not arg.find('@') > 0:
      self.gLogger.error('Not a valid mail address', arg)
      DIRAC.exit(-1)
    self.userMail = arg

  def addUserGroup(self, arg):
    if not arg:
      self.__script.showHelp(exitCode=1)
    if arg not in self.userGroups:
      self.userGroups.append(arg)


@DIRACScript()
def main(self):
  params = Params(self)
  self.registerSwitch('N:', 'UserName:', 'Short Name of the User (Mandatory)', params.setUserName)
  self.registerSwitch('D:', 'UserDN:', 'DN of the User Certificate (Mandatory)', params.setUserDN)
  self.registerSwitch('M:', 'UserMail:', 'eMail of the user (Mandatory)', params.setUserMail)
  self.registerSwitch(
      'G:',
      'UserGroup:',
      'Name of the Group for the User (Allow Multiple instances or None)',
      params.addUserGroup)

  self.parseCommandLine(ignoreErrors=True)

  if params.userName is None or params.userDN is None or params.userMail is None:
    self.showHelp(exitCode=1)

  args = self.getPositionalArgs()

  from DIRAC.Interfaces.API.DiracAdmin import DiracAdmin
  diracAdmin = DiracAdmin()
  exitCode = 0
  errorList = []

  userProps = {'DN': params.userDN, 'Email': params.userMail}
  if params.userGroups:
    userProps['Groups'] = params.userGroups
  for prop in args:
    pl = prop.split("=")
    if len(pl) < 2:
      errorList.append(("in arguments", "Property %s has to include a '=' to separate name from value" % prop))
      exitCode = 255
    else:
      pName = pl[0]
      pValue = "=".join(pl[1:])
      self.gLogger.info("Setting property %s to %s" % (pName, pValue))
      userProps[pName] = pValue

  if not diracAdmin.csModifyUser(params.userName, userProps, createIfNonExistant=True)['OK']:
    errorList.append(("add user", "Cannot register user %s" % params.userName))
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
