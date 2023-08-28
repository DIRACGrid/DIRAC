#!/usr/bin/env python
"""
  Retrieve logging information for a DIRAC job
"""
import DIRAC
from DIRAC import gLogger
from DIRAC.Interfaces.Utilities.DConfigCache import ConfigCache
from DIRAC.Interfaces.Utilities.DCommands import ArrayFormatter
from DIRAC.WorkloadManagementSystem.Client.JobMonitoringClient import JobMonitoringClient
from DIRAC.Core.Base.Script import Script


class Params:
    def __init__(self):
        self.fmt = "pretty"

    def setFmt(self, arg=None):
        self.fmt = arg.lower()

    def getFmt(self):
        return self.fmt


@Script()
def main():
    params = Params()

    Script.registerArgument(["JobID: DIRAC Job ID"])
    Script.registerSwitch("f:", "Fmt=", "display format (pretty, csv, json)", params.setFmt)

    configCache = ConfigCache()
    Script.parseCommandLine(ignoreErrors=True)
    configCache.cacheConfig()

    args = Script.getPositionalArgs()

    exitCode = 0

    jobs = map(int, args)

    monitoring = JobMonitoringClient()
    af = ArrayFormatter(params.getFmt())
    headers = ["Status", "MinorStatus", "ApplicationStatus", "Time", "Source"]
    errors = []
    for job in jobs:
        result = monitoring.getJobLoggingInfo(job)
        if result["OK"]:
            gLogger.notice(af.listFormat(result["Value"], headers, sort=headers.index("Time")))
        else:
            errors.append(result["Message"])
            exitCode = 2

    for error in errors:
        gLogger.error(error)

    DIRAC.exit(exitCode)


if __name__ == "__main__":
    main()
