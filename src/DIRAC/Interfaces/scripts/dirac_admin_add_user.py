#!/usr/bin/env python
"""
Add or Modify a User info in DIRAC

Example:
  $ dirac-admin-add-user -N vhamar -D /O=GRID/C=FR/O=CNRS/OU=CPPM/CN=Vanessa Hamar -M hamar@cppm.in2p3.fr -G dirac_user
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import DIRAC
from DIRAC import gLogger
from DIRAC.Core.Utilities.DIRACScript import DIRACScript


class AddUser(DIRACScript):

  def initParameters(self):
    self.userName = None
    self.userDN = None
    self.userMail = None
    self.userGroups = []

  def setUserName(self, arg):
    if self.userName or not arg:
      self.showHelp(exitCode=1)
    self.userName = arg

  def setUserDN(self, arg):
    if self.userDN or not arg:
      self.showHelp(exitCode=1)
    self.userDN = arg

  def setUserMail(self, arg):
    if self.userMail or not arg:
      self.showHelp(exitCode=1)
    if not arg.find('@') > 0:
      gLogger.error('Not a valid mail address', arg)
      DIRAC.exit(-1)
    self.userMail = arg

  def addUserGroup(self, arg):
    if not arg:
      self.showHelp(exitCode=1)
    if arg not in self.userGroups:
      self.userGroups.append(arg)


@AddUser()
def main(self):
  self.registerSwitch('N:', 'UserName:', 'Short Name of the User (Mandatory)', self.setUserName)
  self.registerSwitch('D:', 'UserDN:', 'DN of the User Certificate (Mandatory)', self.setUserDN)
  self.registerSwitch('M:', 'UserMail:', 'eMail of the user (Mandatory)', self.setUserMail)
  self.registerSwitch(
      'G:',
      'UserGroup:',
      'Name of the Group for the User (Allow Multiple instances or None)',
      self.addUserGroup)
  self.registerArgument(["Property=<Value>: Properties to be added to the User like (Phone=XXXX)"],
                        mandatory=False)
  self.parseCommandLine(ignoreErrors=True)

  if self.userName is None or self.userDN is None or self.userMail is None:
    self.showHelp(exitCode=1)

  args = self.getPositionalArgs()

  from DIRAC.Interfaces.API.DiracAdmin import DiracAdmin
  diracAdmin = DiracAdmin()
  exitCode = 0
  errorList = []

  userProps = {'DN': self.userDN, 'Email': self.userMail}
  if self.userGroups:
    userProps['Groups'] = self.userGroups
  for prop in args:
    pl = prop.split("=")
    if len(pl) < 2:
      errorList.append(("in arguments", "Property %s has to include a '=' to separate name from value" % prop))
      exitCode = 255
    else:
      pName = pl[0]
      pValue = "=".join(pl[1:])
      gLogger.info("Setting property %s to %s" % (pName, pValue))
      userProps[pName] = pValue

  if not diracAdmin.csModifyUser(self.userName, userProps, createIfNonExistant=True)['OK']:
    errorList.append(("add user", "Cannot register user %s" % self.userName))
    exitCode = 255
  else:
    result = diracAdmin.csCommitChanges()
    if not result['OK']:
      errorList.append(("commit", result['Message']))
      exitCode = 255

  for error in errorList:
    gLogger.error("%s: %s" % error)

  DIRAC.exit(exitCode)


if __name__ == "__main__":
  main()  # pylint: disable=no-value-for-parameter
