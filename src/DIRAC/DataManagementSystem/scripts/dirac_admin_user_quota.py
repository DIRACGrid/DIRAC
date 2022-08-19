#!/usr/bin/env python
"""
Show storage quotas for specified users or for all registered users if nobody is specified

Example:
  $ dirac-admin-user-quota
  ------------------------------
  Username       |     Quota (GB)
  ------------------------------
  atsareg        |           None
  msapunov       |           None
  vhamar         |           None
  ------------------------------
"""
import DIRAC
from DIRAC.Core.Base.Script import Script


@Script()
def main():
    # Registering arguments will automatically add their description to the help menu
    Script.registerArgument(["User: list of SEs or comma-separated SEs"], mandatory=False)

    _, users = Script.parseCommandLine()

    from DIRAC import gLogger, gConfig

    if not users:
        res = gConfig.getSections("/Registry/Users")
        if not res["OK"]:
            gLogger.error("Failed to retrieve user list from CS", res["Message"])
            DIRAC.exit(2)
        users = res["Value"]

    gLogger.notice("-" * 30)
    gLogger.notice("{}|{}".format("Username".ljust(15), "Quota (GB)".rjust(15)))
    gLogger.notice("-" * 30)
    for user in sorted(users):
        quota = gConfig.getValue("/Registry/Users/%s/Quota" % user, 0)
        if not quota:
            quota = gConfig.getValue("/Registry/DefaultStorageQuota")
        gLogger.notice(f"{user.ljust(15)}|{str(quota).rjust(15)}")
    gLogger.notice("-" * 30)
    DIRAC.exit(0)


if __name__ == "__main__":
    main()
