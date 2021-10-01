#!/usr/bin/env python
########################################################################
# File :   dirac-agent
# Author : Adria Casajus, Andrei Tsaregorodtsev, Stuart Paterson
########################################################################
"""
This is a script to launch DIRAC agents. Mostly internal.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import sys

from DIRAC import gLogger
from DIRAC.Core.Base.AgentReactor import AgentReactor
from DIRAC.Core.Utilities.DErrno import includeExtensionErrors
from DIRAC.Core.Utilities.DIRACScript import DIRACScript as Script


@Script()
def main():
    Script.registerArgument(["Agent: specify which agent to run"])
    positionalArgs = Script.getPositionalArgs(group=True)
    localCfg = Script.localCfg

    agentName = positionalArgs[0]
    localCfg.setConfigurationForAgent(agentName)
    localCfg.addMandatoryEntry("/DIRAC/Setup")
    localCfg.addDefaultEntry("/DIRAC/Security/UseServerCertificate", "yes")
    localCfg.addDefaultEntry("LogLevel", "INFO")
    localCfg.addDefaultEntry("LogColor", True)
    resultDict = localCfg.loadUserData()
    if not resultDict["OK"]:
        gLogger.error("There were errors when loading configuration", resultDict["Message"])
        sys.exit(1)

    includeExtensionErrors()

    agentReactor = AgentReactor(positionalArgs[0])
    result = agentReactor.loadAgentModules(positionalArgs)
    if result["OK"]:
        agentReactor.go()
    else:
        gLogger.error("Error while loading agent module", result["Message"])
        sys.exit(2)


if __name__ == "__main__":
    main()
