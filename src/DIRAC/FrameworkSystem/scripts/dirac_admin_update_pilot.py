#!/usr/bin/env python
"""
Script to update pilot version in CS
"""
from packaging.version import Version

import DIRAC
from DIRAC.Core.Base.Script import Script


@Script()
def main():
    Script.registerSwitch(
        "v:",
        "vo=",
        "Location of pilot version in CS /Operations/<vo>/Pilot/Version",
    )
    # Registering arguments will automatically add their description to the help menu
    Script.registerArgument("version: pilot version you want to update to")
    Script.parseCommandLine(ignoreErrors=False)

    # parseCommandLine show help when mandatory arguments are not specified or incorrect argument
    version = Script.getPositionalArgs(group=True)

    vo = None
    for switch in Script.getUnprocessedSwitches():
        if switch[0] == "v" or switch[0] == "vo":
            vo = switch[1]

    from DIRAC import S_OK
    from DIRAC import gLogger
    from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
    from DIRAC.ConfigurationSystem.Client.CSAPI import CSAPI

    def updatePilot(version, vo):
        """
        Update in the CS the pilot version used,
        If only one version present in CS it's overwritten.
        If two versions present, the new one is added and the last removed

        :param version: version vArBpC of pilot you want to use
        :param vo: Location of pilot version in CS /Operations/<vo>/Pilot/Version
        """
        pilotVersion = sorted(Operations(vo=vo).getValue("Pilot/Version", []), key=lambda x: Version(x), reverse=True)
        if version in pilotVersion:
            gLogger.warn(f"Version {version} already set in CS")
        elif pilotVersion:
            pilotVersion = [version, pilotVersion[0]]
        else:
            gLogger.warn("No pilot version set in CS")
            pilotVersion = [version]
        api = CSAPI()
        if vo:
            api.setOption(f"Operations/{vo}/Pilot/Version", ", ".join(pilotVersion))
        else:
            api.setOption("Operations/Defaults/Pilot/Version", ", ".join(pilotVersion))
        result = api.commit()
        if not result["OK"]:
            gLogger.fatal("Could not commit new version of pilot!")
            return result

        newVersion = Operations(vo=vo).getValue("Pilot/Version")
        return S_OK(f"New version of pilot set to {newVersion}")

    result = updatePilot(version, vo)
    if not result["OK"]:
        gLogger.fatal(result["Message"])
        DIRAC.exit(1)
    gLogger.notice(result["Value"])
    DIRAC.exit(0)


if __name__ == "__main__":
    main()
