#!/usr/bin/env python
"""
Returns the LFN matching given GUIDs
"""
from DIRAC.Core.Base.Script import Script


@Script()
def main():
    # Registering arguments will automatically add their description to the help menu
    Script.registerArgument("GUIDs: GUIDs separated by a comma")
    Script.parseCommandLine()

    import DIRAC
    from DIRAC import gLogger

    # parseCommandLine show help when mandatory arguments are not specified or incorrect argument
    args = Script.getPositionalArgs()
    guids = args[0]

    try:
        guids = guids.split(",")
    except Exception:
        pass

    from DIRAC.Resources.Catalog.FileCatalog import FileCatalog

    fc = FileCatalog()
    res = fc.getLFNForGUID(guids)
    if not res["OK"]:
        gLogger.error("Failed to get the LFNs", res["Message"])
        DIRAC.exit(-2)

    errorGuid = {}
    for guid, reason in res["Value"]["Failed"].items():
        errorGuid.setdefault(reason, []).append(guid)

    for error, guidList in errorGuid.items():
        gLogger.notice(f"Error '{error}' for guids {guidList}")

    for guid, lfn in res["Value"]["Successful"].items():
        gLogger.notice(f"{guid} -> {lfn}")

    DIRAC.exit(0)


if __name__ == "__main__":
    main()
