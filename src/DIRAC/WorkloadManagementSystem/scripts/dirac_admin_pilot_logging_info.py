#! /usr/bin/env python
"""
Get Pilots Logging for specific Pilot UUID or Job ID.

WARNING: Only one option (either uuid or jobid) should be used.
"""
import DIRAC
from DIRAC import S_OK, gLogger
from DIRAC.Core.Base.Script import Script

uuid = None
jobid = None


def setUUID(optVal):
    """
    Set UUID from arguments
    """
    global uuid
    uuid = optVal
    return S_OK()


def setJobID(optVal):
    """
    Set JobID from arguments
    """
    global jobid
    jobid = optVal
    return S_OK()


@Script()
def main():
    global uuid
    global jobid
    Script.registerSwitch("u:", "uuid=", "get PilotsLogging for given Pilot UUID", setUUID)
    Script.registerSwitch("j:", "jobid=", "get PilotsLogging for given Job ID", setJobID)
    Script.parseCommandLine()

    from DIRAC.WorkloadManagementSystem.Client.PilotManagerClient import PilotManagerClient

    if jobid:
        result = PilotManagerClient().getPilots(jobid)
        if not result["OK"]:
            gLogger.error(result["Message"])
            DIRAC.exit(1)
        gLogger.debug(result["Value"])
        uuid = list(result["Value"])[0]

    result = PilotManagerClient().getPilotLoggingInfo(uuid)
    if not result["OK"]:
        gLogger.error(result["Message"])
        DIRAC.exit(1)
    gLogger.notice(result["Value"])

    DIRAC.exit(0)


if __name__ == "__main__":
    main()
