#!/usr/bin/env python
########################################################################
# File :    dirac-admin-service-ports
# Author :  Stuart Paterson
########################################################################
"""
Print the service ports for the specified setup

Example:
  $ dirac-admin-service-ports
  {'Framework/ProxyManager': 9152,
   'Framework/SystemAdministrator': 9162,
   'Framework/UserProfileManager': 9155,
   'WorkloadManagement/JobManager': 9132,
   'WorkloadManagement/PilotManager': 9171,
   'WorkloadManagement/Matcher': 9170,
   'WorkloadManagement/SandboxStore': 9196,
   'WorkloadManagement/WMSAdministrator': 9145}
"""
import DIRAC
from DIRAC.Core.Base.Script import Script


@Script()
def main():
    # Registering arguments will automatically add their description to the help menu
    Script.registerArgument("Setup:    Name of the setup", default="", mandatory=False)
    Script.parseCommandLine(ignoreErrors=True)
    setup = Script.getPositionalArgs(group=True)

    from DIRAC.Interfaces.API.DiracAdmin import DiracAdmin

    diracAdmin = DiracAdmin()
    result = diracAdmin.getServicePorts(setup, printOutput=True)
    if result["OK"]:
        DIRAC.exit(0)
    else:
        print(result["Message"])
        DIRAC.exit(2)


if __name__ == "__main__":
    main()
