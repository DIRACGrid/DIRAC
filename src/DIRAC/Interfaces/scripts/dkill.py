#!/usr/bin/env python
"""
  Kill or delete DIRAC job
"""
import DIRAC
from DIRAC import gLogger
from DIRAC.Interfaces.Utilities.DConfigCache import ConfigCache
from DIRAC.Core.Base.Script import Script
from DIRAC.Interfaces.Utilities.DCommands import DSession


class Params:
    def __init__(self):
        self.delete = False
        self.selectAll = False
        self.verbose = False

    def setDelete(self, arg=None):
        self.delete = True

    def getDelete(self):
        return self.delete

    def setSelectAll(self, arg=None):
        self.selectAll = True

    def getSelectAll(self):
        return self.selectAll

    def setVerbose(self, arg=None):
        self.verbose = True

    def getVerbose(self):
        return self.verbose


@Script()
def main():
    params = Params()

    Script.registerArgument(["JobID: DIRAC Job ID"])
    Script.registerSwitch("D", "delete", "delete job", params.setDelete)
    Script.registerSwitch("a", "all", "select all jobs", params.setSelectAll)
    Script.registerSwitch("v", "verbose", "verbose output", params.setVerbose)

    configCache = ConfigCache()
    Script.parseCommandLine(ignoreErrors=True)
    configCache.cacheConfig()

    jobs = Script.getPositionalArgs()

    exitCode = 0

    from DIRAC.WorkloadManagementSystem.Client.WMSClient import WMSClient
    from DIRAC.WorkloadManagementSystem.Client.JobMonitoringClient import (
        JobMonitoringClient,
    )

    wmsClient = WMSClient()

    if params.getSelectAll():
        session = DSession()
        result = session.getUserName()
        if result["OK"]:
            userName = result["Value"]

            monitoring = JobMonitoringClient()
            result = monitoring.getJobs({"Owner": userName})
            if not result["OK"]:
                gLogger.error(result["Message"])
            else:
                jobs += map(int, result["Value"])
        else:
            gLogger.error(result["Message"])

    errors = []
    for job in jobs:
        result = None
        if params.delete:
            result = wmsClient.deleteJob(job)
        else:
            result = wmsClient.killJob(job)
        if not result["OK"]:
            errors.append(result["Message"])
            exitCode = 2
        elif params.getVerbose():
            action = "killed"
            if params.getDelete():
                action = "deleted"
            gLogger.notice(f"{action} job {job}")

    for error in errors:
        gLogger.error(str(error))

    DIRAC.exit(exitCode)


if __name__ == "__main__":
    main()
