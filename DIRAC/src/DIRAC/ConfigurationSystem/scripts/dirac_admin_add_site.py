#!/usr/bin/env python
########################################################################
# File :   dirac-admin-add-site
# Author : Federico Stagni
########################################################################
"""
Add a new DIRAC SiteName to DIRAC Configuration, including one or more CEs.
If site is already in the CS with another name, error message will be produced.
If site is already in the CS with the right name, only new CEs will be added.

Example:
  $ dirac-admin-add-site LCG.IN2P3.fr IN2P3-Site ce01.in2p3.fr
"""
from DIRAC.Core.Base.Script import Script
from DIRAC import exit as DIRACExit, gLogger
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getDIRACSiteName
from DIRAC.ConfigurationSystem.Client.CSAPI import CSAPI


@Script()
def main():
    # Registering arguments will automatically add their description to the help menu
    Script.registerArgument(
        "DIRACSiteName: Name of the site for DIRAC in the form GRID.LOCATION.COUNTRY " "(ie: LCG.CERN.ch)"
    )
    Script.registerArgument("GridSiteName:  Name of the site in the Grid (ie: CERN-PROD)")
    Script.registerArgument(["CE:           Name of the CE to be included in the site (ie: ce111.cern.ch)"])
    Script.parseCommandLine(ignoreErrors=True)

    diracSiteName, gridSiteName, ces = Script.getPositionalArgs(group=True)

    try:
        diracGridType, place, country = diracSiteName.split(".")
    except ValueError:
        gLogger.error("The DIRACSiteName should be of the form GRID.LOCATION.COUNTRY for example LCG.CERN.ch")
        DIRACExit(-1)

    result = getDIRACSiteName(gridSiteName)
    newSite = True
    if result["OK"] and result["Value"]:
        if len(result["Value"]) > 1:
            gLogger.notice(f"{gridSiteName} GOC site name is associated with several DIRAC sites:")
            for i, dSite in enumerate(result["Value"]):
                gLogger.notice("%d: %s" % (i, dSite))
            inp = input("Enter your choice number: ")
            try:
                inp = int(inp)
            except ValueError:
                gLogger.error("You should enter an integer number")
                DIRACExit(-1)
            if 0 <= inp < len(result["Value"]):
                diracCSSite = result["Value"][inp]
            else:
                gLogger.error("Number out of range: %d" % inp)
                DIRACExit(-1)
        else:
            diracCSSite = result["Value"][0]
        if diracCSSite == diracSiteName:
            gLogger.notice(f"Site with GOC name {gridSiteName} is already defined as {diracSiteName}")
            newSite = False
        else:
            gLogger.error(f"ERROR: Site with GOC name {gridSiteName} is already defined as {diracCSSite}")
            DIRACExit(-1)
    else:
        gLogger.error(f"ERROR getting DIRAC site name of {gridSiteName}", result.get("Message"))

    csAPI = CSAPI()

    if newSite:
        gLogger.notice(f"Site to CS: {diracSiteName}")
        res = csAPI.addSite(diracSiteName, {"Name": gridSiteName})
        if not res["OK"]:
            gLogger.error("Failed adding site to CS", res["Message"])
            DIRACExit(1)
        res = csAPI.commit()
        if not res["OK"]:
            gLogger.error("Failure committing to CS", res["Message"])
            DIRACExit(3)

    for ce in ces:
        gLogger.notice(f"Adding CE {ce}")
        res = csAPI.addCEtoSite(diracSiteName, ce)
        if not res["OK"]:
            gLogger.error(f"Failed adding CE {ce} to CS", res["Message"])
            DIRACExit(2)
        res = csAPI.commit()
        if not res["OK"]:
            gLogger.error("Failure committing to CS", res["Message"])
            DIRACExit(3)


if __name__ == "__main__":
    main()
