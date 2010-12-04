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
import DIRAC
from DIRAC.ConfigurationSystem.Client.LocalConfiguration import LocalConfiguration
from DIRAC import gLogger, gConfig
from DIRAC.Core.Base.AgentReactor import AgentReactor

localCfg = LocalConfiguration()

positionalArgs = localCfg.getPositionalArguments()
if len( positionalArgs ) == 0:
  gLogger.initialize( "NOT SPECIFIED", "/" )
  gLogger.fatal( "You must specify which agent to run!" )
  sys.exit( 1 )

if len( positionalArgs ) == 1:
  agentName = positionalArgs[0]
  multiAgent = False
else:
  agentName = "Framework/MultiAgent"
  multiAgent = True

localCfg.setConfigurationForAgent( agentName )
localCfg.addMandatoryEntry( "/DIRAC/Setup" )
localCfg.addDefaultEntry( "/DIRAC/Security/UseServerCertificate", "yes" )
resultDict = localCfg.loadUserData()
if not resultDict[ 'OK' ]:
  gLogger.error( "There were errors when loading configuration", resultDict[ 'Message' ] )
  sys.exit( 1 )

ar = AgentReactor( agentName )
result = ar.loadAgentModules( positionalArgs )
if not result[ 'OK' ]:
  gLogger.error( "Error while loading agent module: %s" % result[ 'Message' ] )
  sys.exit( 1 )
ar.go()

