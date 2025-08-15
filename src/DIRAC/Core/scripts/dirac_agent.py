#!/usr/bin/env python
########################################################################
# File :   dirac-agent
# Author : Adria Casajus, Andrei Tsaregorodtsev, Stuart Paterson
########################################################################
"""
This is a script to launch DIRAC agents. Mostly internal.
"""
import os
import sys

from DIRAC import gLogger
from DIRAC.Core.Base.AgentReactor import AgentReactor
from DIRAC.Core.Utilities.DErrno import includeExtensionErrors
from DIRAC.Core.Base.Script import Script


@Script()
def main():
    Script.registerArgument(["Agent: specify which agent to run"])
    positionalArgs = Script.getPositionalArgs(group=True)
    localCfg = Script.localCfg

    agentName = positionalArgs[0]
    localCfg.setConfigurationForAgent(agentName)
    localCfg.addDefaultEntry("/DIRAC/Security/UseServerCertificate", "yes")
    localCfg.addDefaultEntry("LogColor", True)
    resultDict = localCfg.loadUserData(requireSuccessfulSync=True)
    if not resultDict["OK"]:
        gLogger.error("There were errors when loading configuration", resultDict["Message"])
        sys.exit(1)

    includeExtensionErrors()

    agentReactor = AgentReactor(positionalArgs[0])
    result = agentReactor.loadAgentModules(positionalArgs)
    if result["OK"]:
        agentReactor.go()
        # dirac_agent might interact with ARC library which cannot be closed using a simple sys.exit(0)
        # See https://bugzilla.nordugrid.org/show_bug.cgi?id=4022 for further details
        os._exit(0)
    else:
        gLogger.error("Error while loading agent module", result["Message"])
        sys.exit(2)


if __name__ == "__main__":
    main()
