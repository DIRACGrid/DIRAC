#!/usr/bin/env python
"""
Script to update pilot version in CS
"""
import DIRAC
from DIRAC.Core.Base.Script import Script


@Script()
def main():
    Script.registerSwitch(
        "v:",
        "vo=",
        "Location of pilot version in CS /Operations/<vo>/Pilot/Version"
        " (default value specified in CS under /DIRAC/DefaultSetup)",
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

    from DIRAC import S_OK, S_ERROR
    from DIRAC import gConfig, gLogger
    from DIRAC.ConfigurationSystem.Client.CSAPI import CSAPI

    def updatePilot(version, vo):
        """
        Update in the CS the pilot version used,
        If only one version present in CS it's overwritten.
        If two versions present, the new one is added and the last removed

        :param version: version vArBpC of pilot you want to use
        :param vo: Location of pilot version in CS /Operations/<vo>/Pilot/Version
        """
        setup = vo
        if not vo:
            setup = gConfig.getValue("/DIRAC/DefaultSetup")
        if not setup:
            return S_ERROR("No value set for /DIRAC/DefaultSetup in CS")

        pilotVersion = gConfig.getValue("Operations/%s/Pilot/Version" % setup, [])
        if not pilotVersion:
            return S_ERROR("No pilot version set under Operations/%s/Pilot/Version in CS" % setup)

        pilotVersion.pop()
        pilotVersion.insert(0, version)
        api = CSAPI()
        api.setOption("Operations/%s/Pilot/Version" % setup, ", ".join(pilotVersion))
        result = api.commit()
        if not result["OK"]:
            gLogger.fatal("Could not commit new version of pilot!")
            return result

        newVersion = gConfig.getValue("Operations/%s/Pilot/Version" % setup)
        return S_OK("New version of pilot set to %s" % newVersion)

    result = updatePilot(version, vo)
    if not result["OK"]:
        gLogger.fatal(result["Message"])
        DIRAC.exit(1)
    gLogger.notice(result["Value"])
    DIRAC.exit(0)


if __name__ == "__main__":
    main()
