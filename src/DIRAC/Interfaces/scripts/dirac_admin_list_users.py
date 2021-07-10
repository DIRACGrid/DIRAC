#!/usr/bin/env python
########################################################################
# File :    dirac-admin-list-users
# Author :  Adrian Casajus
########################################################################
"""
Lists the users in the Configuration. If no group is specified return all users.

Example:
  $ dirac-admin-list-users
  All users registered:
  vhamar
  msapunov
  atsareg
"""
from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

__RCSID__ = "$Id$"

from DIRAC.Core.Utilities.DIRACScript import DIRACScript


@DIRACScript()
def main(self):
  self.registerSwitch("e", "extended", "Show extended info")
  # Registering arguments will automatically add their description to the help menu
  self.registerArgument(["Group:    Only users from this group (default: all)"],
                        default=['all'], mandatory=False)
  self.parseCommandLine(ignoreErrors=True)
  groups = self.getPositionalArgs(group=True)

  import DIRAC
  from DIRAC.Interfaces.API.DiracAdmin import DiracAdmin
  diracAdmin = DiracAdmin()
  exitCode = 0
  errorList = []
  extendedInfo = False

  for unprocSw in self.getUnprocessedSwitches():
    if unprocSw[0] in ('e', 'extended'):
      extendedInfo = True

  def printUsersInGroup(group=False):
    result = diracAdmin.csListUsers(group)
    if result['OK']:
      if group:
        print("Users in group %s:" % group)
      else:
        print("All users registered:")
      for username in result['Value']:
        print(" %s" % username)

  def describeUsersInGroup(group=False):
    result = diracAdmin.csListUsers(group)
    if result['OK']:
      if group:
        print("Users in group %s:" % group)
      else:
        print("All users registered:")
      result = diracAdmin.csDescribeUsers(result['Value'])
      print(diracAdmin.pPrint.pformat(result['Value']))

  for group in groups:
    if 'all' in groups:
      group = False
    if not extendedInfo:
      printUsersInGroup(group)
    else:
      describeUsersInGroup(group)

  for error in errorList:
    print("ERROR %s: %s" % error)

  DIRAC.exit(exitCode)


if __name__ == "__main__":
  main()  # pylint: disable=no-value-for-parameter
