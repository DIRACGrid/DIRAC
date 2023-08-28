#!/usr/bin/env python
########################################################################
# File :   dirac_admin_add_shifter
# Author : Federico Stagni
########################################################################
"""
Adds or modify a shifter, in the operations section of the CS
"""
from DIRAC.Core.Base.Script import Script
from DIRAC.ConfigurationSystem.Client.CSAPI import CSAPI
from DIRAC import exit as DIRACExit, gLogger


@Script()
def main():
    # Registering arguments will automatically add their description to the help menu
    Script.registerArgument("ShifterRole:  Name of the shifter role, e.g. DataManager")
    Script.registerArgument("UserName:     A user name, as registered in Registry section")
    Script.registerArgument("DIRACGroup:   DIRAC Group, e.g. diracAdmin (the user has to have this role)")
    Script.parseCommandLine(ignoreErrors=True)

    csAPI = CSAPI()

    shifterRole, userName, diracGroup = Script.getPositionalArgs(group=True)
    res = csAPI.addShifter({shifterRole: {"User": userName, "Group": diracGroup}})
    if not res["OK"]:
        gLogger.error("Could not add shifter", ": " + res["Message"])
        DIRACExit(1)
    res = csAPI.commit()
    if not res["OK"]:
        gLogger.error("Could not add shifter", ": " + res["Message"])
        DIRACExit(1)
    gLogger.notice(f"Added shifter {shifterRole} as user {userName} with group {diracGroup}")


if __name__ == "__main__":
    main()
