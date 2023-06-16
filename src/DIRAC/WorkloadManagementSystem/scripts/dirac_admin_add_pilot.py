#!/usr/bin/env python
"""
This command is here mainly to be used by running Pilots.
Its goal is to add a PilotReference in PilotAgentsDB, and to update its status.

While SiteDirectors normally add pilots in PilotAgentsDB,
the same can't be true for pilots started in the vacuum (i.e. without SiteDirectors involved).
This script is here to solve specifically this issue, even though it can be used for other things too.

Example:
  $ dirac-admin-add-pilot htcondor:123456 user_DN user_group DIRAC A11D8D2E-60F8-17A6-5520-E2276F41 --Status=Running

"""

from DIRAC import S_OK, S_ERROR, gLogger, exit as DIRACExit
from DIRAC.Core.Base.Script import Script


class Params:
    """
    Class holding the parameters, and callbacks for their respective switches.
    """

    def __init__(self):
        """C'or"""
        self.status = False
        self.taskQueueID = 0
        self.switches = [
            ("", "status=", "sets the pilot status", self.setStatus),
            ("t:", "taskQueueID=", "sets the taskQueueID", self.setTaskQueueID),
        ]

    def setStatus(self, value):
        """sets self.status

        :param value: option argument

        :return: S_OK()/S_ERROR()
        """
        from DIRAC.WorkloadManagementSystem.Client.PilotStatus import PILOT_STATES

        if value not in PILOT_STATES:
            return S_ERROR(f"{value} is not a valid pilot status")
        self.status = value
        return S_OK()

    def setTaskQueueID(self, value):
        """sets self.taskQueueID

        :param value: option argument

        :return: S_OK()/S_ERROR()
        """
        try:
            self.taskQueueID = int(value)
        except ValueError:
            return S_ERROR("TaskQueueID has to be a number")
        return S_OK()


@Script()
def main():
    """
    This is the script main method, which holds all the logic.
    """
    params = Params()

    Script.registerSwitches(params.switches)
    Script.registerArgument("pilotRef: pilot reference")
    Script.registerArgument("ownerDN: pilot owner DN")
    Script.registerArgument("ownerGroup: pilot owner group")
    Script.registerArgument("gridType: grid type")
    Script.registerArgument("pilotStamp: DIRAC pilot stamp")

    Script.parseCommandLine(ignoreErrors=False)

    # Get grouped positional arguments
    pilotRef, ownerDN, ownerGroup, gridType, pilotStamp = Script.getPositionalArgs(group=True)

    # Import the required DIRAC modules
    from DIRAC.Core.Utilities import DErrno
    from DIRAC.WorkloadManagementSystem.Client.PilotManagerClient import PilotManagerClient

    pmc = PilotManagerClient()

    # Check if pilot is not already registered
    res = pmc.getPilotInfo(pilotRef)
    if not res["OK"]:
        if not DErrno.cmpError(res, DErrno.EWMSNOPILOT):
            gLogger.error(res["Message"])
            DIRACExit(1)
        res = pmc.addPilotTQReference(
            [pilotRef], params.taskQueueID, ownerDN, ownerGroup, "Unknown", gridType, {pilotRef: pilotStamp}
        )
        if not res["OK"]:
            gLogger.error(res["Message"])
            DIRACExit(1)

    if params.status:
        res = pmc.setPilotStatus(pilotRef, params.status)
        if not res["OK"]:
            gLogger.error(res["Message"])
            DIRACExit(1)

    DIRACExit(0)


if __name__ == "__main__":
    main()
