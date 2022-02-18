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
from DIRAC.Core.Base.Script import Script


@Script()
def main():
    Script.registerSwitch("e", "extended", "Show extended info")
    # Registering arguments will automatically add their description to the help menu
    Script.registerArgument(["Group:    Only users from this group (default: all)"], default=["all"], mandatory=False)
    Script.parseCommandLine(ignoreErrors=True)
    args = Script.getPositionalArgs(group=True)

    import DIRAC
    from DIRAC.Interfaces.API.DiracAdmin import DiracAdmin

    diracAdmin = DiracAdmin()
    exitCode = 0
    errorList = []
    extendedInfo = False

    for unprocSw in Script.getUnprocessedSwitches():
        if unprocSw[0] in ("e", "extended"):
            extendedInfo = True

    def printUsersInGroup(group=False):
        result = diracAdmin.csListUsers(group)
        if result["OK"]:
            if group:
                print("Users in group %s:" % group)
            else:
                print("All users registered:")
            for username in result["Value"]:
                print(" %s" % username)

    def describeUsersInGroup(group=False):
        result = diracAdmin.csListUsers(group)
        if result["OK"]:
            if group:
                print("Users in group %s:" % group)
            else:
                print("All users registered:")
            result = diracAdmin.csDescribeUsers(result["Value"])
            print(diracAdmin.pPrint.pformat(result["Value"]))

    for group in args:
        if "all" in args:
            group = False
        if not extendedInfo:
            printUsersInGroup(group)
        else:
            describeUsersInGroup(group)

    for error in errorList:
        print("ERROR %s: %s" % error)

    DIRAC.exit(exitCode)


if __name__ == "__main__":
    main()
