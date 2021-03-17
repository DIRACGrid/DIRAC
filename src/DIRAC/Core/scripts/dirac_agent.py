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
from DIRAC.Core.Utilities.DIRACScript import DIRACScript
from DIRAC.ConfigurationSystem.Client.LocalConfiguration import LocalConfiguration


@DIRACScript()
def main(self):
  self.localCfg.registerCmdArg(["Agent: specify which agent to run"])
  agents = self.localCfg.getPositionalArguments(group=True)

  agentName = agents[0]
  self.localCfg.setConfigurationForAgent(agentName)
  self.localCfg.addMandatoryEntry("/DIRAC/Setup")
  self.localCfg.addDefaultEntry("/DIRAC/Security/UseServerCertificate", "yes")
  self.localCfg.addDefaultEntry("LogLevel", "INFO")
  self.localCfg.addDefaultEntry("LogColor", True)
  resultDict = self.localCfg.loadUserData()
  if not resultDict['OK']:
    gLogger.error("There were errors when loading configuration", resultDict['Message'])
    sys.exit(1)

  includeExtensionErrors()

  agentReactor = AgentReactor(agentName)
  result = agentReactor.loadAgentModules(agents)
  if result['OK']:
    agentReactor.go()
  else:
    gLogger.error("Error while loading agent module", result['Message'])
    sys.exit(2)


if __name__ == "__main__":
  main()  # pylint: disable=no-value-for-parameter
