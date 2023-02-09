#!/usr/bin/env python
"""
Add or Modify a Group info in DIRAC

Example:
  $ dirac-admin-add-group -G dirac_test
"""
# pylint: disable=wrong-import-position

import DIRAC
from DIRAC import gLogger
from DIRAC.Core.Base.Script import Script

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


@Script()
def main():
    global groupName
    global groupProperties
    global userNames
    Script.registerSwitch("G:", "GroupName:", "Name of the Group (Mandatory)", setGroupName)
    Script.registerSwitch(
        "U:", "UserName:", "Short Name of user to be added to the Group (Allow Multiple instances or None)", addUserName
    )
    Script.registerSwitch(
        "P:", "Property:", "Property to be added to the Group (Allow Multiple instances or None)", addProperty
    )
    # Registering arguments will automatically add their description to the help menu
    Script.registerArgument(
        ["Property=<Value>: Other properties to be added to the Group like (VOMSRole=XXXX)"], mandatory=False
    )

    _, args = Script.parseCommandLine(ignoreErrors=True)

    if groupName is None:
        Script.showHelp(exitCode=1)

    from DIRAC.Interfaces.API.DiracAdmin import DiracAdmin

    diracAdmin = DiracAdmin()
    exitCode = 0
    errorList = []

    groupProps = {}
    if userNames:
        groupProps["Users"] = ", ".join(userNames)
    if groupProperties:
        groupProps["Properties"] = ", ".join(groupProperties)

    for prop in args:
        pl = prop.split("=")
        if len(pl) < 2:
            errorList.append(("in arguments", f"Property {prop} has to include a '=' to separate name from value"))
            exitCode = 255
        else:
            pName = pl[0]
            pValue = "=".join(pl[1:])
            gLogger.info(f"Setting property {pName} to {pValue}")
            groupProps[pName] = pValue

    if not diracAdmin.csModifyGroup(groupName, groupProps, createIfNonExistant=True)["OK"]:
        errorList.append(("add group", f"Cannot register group {groupName}"))
        exitCode = 255
    else:
        result = diracAdmin.csCommitChanges()
        if not result["OK"]:
            errorList.append(("commit", result["Message"]))
            exitCode = 255

    for error in errorList:
        gLogger.error("%s: %s" % error)

    DIRAC.exit(exitCode)


if __name__ == "__main__":
    main()
