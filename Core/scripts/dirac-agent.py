#!/usr/bin/env python
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/scripts/dirac-agent.py,v 1.2 2008/12/02 10:17:47 acasajus Exp $
# File :   dirac-agent
# Author : Adria Casajus, Andrei Tsaregorodtsev, Stuart Paterson
########################################################################
__RCSID__   = "$Id: dirac-agent.py,v 1.2 2008/12/02 10:17:47 acasajus Exp $"
__VERSION__ = "$Revision: 1.2 $"

"""  This is a script to launch DIRAC agents
"""

import sys
from DIRACEnvironment import DIRAC
from DIRAC.ConfigurationSystem.Client.LocalConfiguration import LocalConfiguration
from DIRAC import gLogger, gConfig
from DIRAC.Core.Base.Agent import createAgent
from DIRAC.Core.Base.AgentReactor import AgentReactor

localCfg = LocalConfiguration()

positionalArgs = localCfg.getPositionalArguments()
if len( positionalArgs ) == 0:
  gLogger.initialize( "NOT SPECIFIED", "/" )
  gLogger.fatal( "You must specify which agent to run!" )
  sys.exit(1)

agentName = positionalArgs[0]
localCfg.setConfigurationForAgent( agentName )
localCfg.addMandatoryEntry( "/DIRAC/Setup" )
localCfg.addDefaultEntry( "/DIRAC/Security/UseServerCertificate", "yes" )
resultDict = localCfg.loadUserData()
if not resultDict[ 'OK' ]:
  gLogger.error( "There were errors when loading configuration", resultDict[ 'Message' ] )
  sys.exit(1)

if len( positionalArgs ) == 1:
  mainName = positionalArgs[0]
else:
  mainName = "Framework/MultiAgent"

agentReactor = AgentReactor( mainName )
#result = agentReactor.loadAgentModules( positionalArgs )
result = agentReactor.loadAgentModules( positionalArgs )
if result[ 'OK' ]:
  agentReactor.go()
else:
  gLogger.error( "Error while loading agent module", result[ 'Message' ] )
  gLogger.info( "Let's try the old agent framework" )
  agent = createAgent(agentName)
  agent.run()
