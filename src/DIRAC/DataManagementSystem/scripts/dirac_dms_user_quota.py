#!/usr/bin/env python
"""
Get the currently defined user data volume quotas

Usage:
  dirac-dms-user-quota [options]

Example:
  $ dirac-dms-user-quota
  Current quota found to be 0.0 GB
"""
from DIRAC.Core.Base.Script import Script


@Script()
def main():
    Script.parseCommandLine(ignoreErrors=False)

    import DIRAC
    from DIRAC import gLogger, gConfig
    from DIRAC.Core.Security.ProxyInfo import getProxyInfo

    res = getProxyInfo(False, False)
    if not res["OK"]:
        gLogger.error("Failed to get client proxy information.", res["Message"])
        DIRAC.exit(2)
    proxyInfo = res["Value"]
    username = proxyInfo["username"]

    try:
        quota = gConfig.getValue("/Registry/DefaultStorageQuota", 0.0)
        quota = gConfig.getValue("/Registry/Users/%s/Quota" % username, quota)
        gLogger.notice("Current quota found to be %.1f GB" % quota)
        DIRAC.exit(0)
    except Exception as x:
        gLogger.exception("Failed to convert retrieved quota", "", x)
        DIRAC.exit(-1)


if __name__ == "__main__":
    main()
