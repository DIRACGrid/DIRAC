#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :   dirac-agent
# Author : Adria Casajus, Andrei Tsaregorodtsev, Stuart Paterson
########################################################################
__RCSID__ = "$Id$"

"""  This is a script to launch DIRAC agents
"""

import sys
from DIRAC.ConfigurationSystem.Client.LocalConfiguration import LocalConfiguration
from DIRAC import gLogger, gConfig
from DIRAC.Core.Base.AgentReactor import AgentReactor

localCfg = LocalConfiguration()

positionalArgs = localCfg.getPositionalArguments()
if len( positionalArgs ) == 0:
  gLogger.fatal( "You must specify which agent to run!" )
  sys.exit( 1 )

agentName = positionalArgs[0]
localCfg.setConfigurationForAgent( agentName )
localCfg.addMandatoryEntry( "/DIRAC/Setup" )
localCfg.addDefaultEntry( "/DIRAC/Security/UseServerCertificate", "yes" )
localCfg.addDefaultEntry( "LogLevel", "INFO" )
localCfg.addDefaultEntry( "LogColor", True )
resultDict = localCfg.loadUserData()
if not resultDict[ 'OK' ]:
  gLogger.error( "There were errors when loading configuration", resultDict[ 'Message' ] )
  sys.exit( 1 )

if len( positionalArgs ) == 1:
  mainName = positionalArgs[0]
else:
  mainName = "Framework/MultiAgent"

agentReactor = AgentReactor( mainName )
result = agentReactor.loadAgentModules( positionalArgs )
if result[ 'OK' ]:
  agentReactor.go()
else:
  gLogger.error( "Error while loading agent module", result[ 'Message' ] )
