#!/bin/env python
"""
List the number of requests in the caches of all the ReqProxyies
"""
import DIRAC
from DIRAC.Core.Base.Script import Script


@Script()
def main():
    Script.registerSwitch("", "Full", "   Print full list of requests")
    Script.parseCommandLine()
    from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient

    fullPrint = False

    for switch in Script.getUnprocessedSwitches():
        if switch[0] == "Full":
            fullPrint = True

    reqClient = ReqClient()

    for server, rpcClient in reqClient.requestProxies().items():
        DIRAC.gLogger.always(f"Checking request cache at {server}")
        reqCache = rpcClient.listCacheDir()
        if not reqCache["OK"]:
            DIRAC.gLogger.error("Cannot list request cache", reqCache)
            continue
        reqCache = reqCache["Value"]

        if not reqCache:
            DIRAC.gLogger.always("No request in cache")
        else:
            if fullPrint:
                DIRAC.gLogger.always("List of requests", reqCache)
            else:
                DIRAC.gLogger.always("Number of requests in the cache", len(reqCache))

    DIRAC.exit(0)


if __name__ == "__main__":
    main()
