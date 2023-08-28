#!/usr/bin/env python
########################################################################
# File :    dirac-admin-delete-user
# Author :  Adrian Casajus
########################################################################
"""
Remove User from Configuration

Example:
  $ dirac-admin-delete-user vhamar
"""
from DIRAC.Core.Base.Script import Script


@Script()
def main():
    # Registering arguments will automatically add their description to the help menu
    Script.registerArgument(["User:     User name"])
    # parseCommandLine show help when mandatory arguments are not specified or incorrect argument
    _, args = Script.parseCommandLine(ignoreErrors=True)

    from DIRAC import exit as DIRACExit
    from DIRAC.Interfaces.API.DiracAdmin import DiracAdmin

    diracAdmin = DiracAdmin()
    exitCode = 0
    errorList = []

    choice = input(f"Are you sure you want to delete user/s {', '.join(args)}? yes/no [no]: ")
    choice = choice.lower()
    if choice not in ("yes", "y"):
        print("Delete aborted")
        DIRACExit(0)

    for user in args:
        if not diracAdmin.csDeleteUser(user):
            errorList.append(("delete user", f"Cannot delete user {user}"))
            exitCode = 255

    if not exitCode:
        result = diracAdmin.csCommitChanges()
        if not result["OK"]:
            errorList.append(("commit", result["Message"]))
            exitCode = 255

    for error in errorList:
        print("ERROR %s: %s" % error)

    DIRACExit(exitCode)


if __name__ == "__main__":
    main()
