#!/usr/bin/env python
########################################################################
# File :    dirac-admin-modify-user
# Author :  Adrian Casajus
########################################################################
"""
Modify a user in the CS.

Example:
  $ dirac-admin-modify-user vhamar /C=FR/O=Org/CN=User dirac_user
"""
import DIRAC
from DIRAC.Core.Base.Script import Script


@Script()
def main():
    Script.registerSwitch("p:", "property=", "Add property to the user <name>=<value>")
    Script.registerSwitch("f", "force", "create the user if it doesn't exist")
    # Registering arguments will automatically add their description to the help menu
    Script.registerArgument(" user:     User name")
    Script.registerArgument(" DN:       DN of the User")
    Script.registerArgument(["group:    Add the user to the group"])
    Script.parseCommandLine(ignoreErrors=True)

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
                errorList.append(
                    ("in arguments", "Property %s has to include a '=' to separate name from value" % prop)
                )
                exitCode = 255
            else:
                pName = pl[0]
                pValue = "=".join(pl[1:])
                print(f"Setting property {pName} to {pValue}")
                userProps[pName] = pValue

    userName, userProps["DN"], userProps["Groups"] = Script.getPositionalArgs(group=True)

    if not diracAdmin.csModifyUser(userName, userProps, createIfNonExistant=forceCreation):
        errorList.append(("modify user", "Cannot modify user %s" % userName))
        exitCode = 255
    else:
        result = diracAdmin.csCommitChanges()
        if not result["OK"]:
            errorList.append(("commit", result["Message"]))
            exitCode = 255

    for error in errorList:
        print("ERROR %s: %s" % error)

    DIRAC.exit(exitCode)


if __name__ == "__main__":
    main()
