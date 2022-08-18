#!/bin/env python
"""
Show ReqDB summary
"""
from DIRAC.Core.Base.Script import Script


@Script()
def main():
    Script.parseCommandLine()

    import DIRAC

    from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient

    reqClient = ReqClient()

    dbSummary = reqClient.getDBSummary()
    if not dbSummary["OK"]:
        DIRAC.gLogger.error(dbSummary["Message"])
        DIRAC.exit(-1)

    dbSummary = dbSummary["Value"]
    if not dbSummary:
        DIRAC.gLogger.info("ReqDB is empty!")
        DIRAC.exit(0)

    reqs = dbSummary.get("Request", {})
    ops = dbSummary.get("Operation", {})
    fs = dbSummary.get("File", {})

    DIRAC.gLogger.always("Requests:")
    for reqState, reqCount in sorted(reqs.items()):
        DIRAC.gLogger.always(f"- '{reqState}' {reqCount}")
    DIRAC.gLogger.always("Operations:")
    for opType, opDict in sorted(ops.items()):
        DIRAC.gLogger.always("- '%s':" % opType)
        for opState, opCount in sorted(opDict.items()):
            DIRAC.gLogger.always(f"  - '{opState}' {opCount}")
    DIRAC.gLogger.always("Files:")
    for fState, fCount in sorted(fs.items()):
        DIRAC.gLogger.always(f"- '{fState}' {fCount}")

    DIRAC.exit(0)


if __name__ == "__main__":
    main()
