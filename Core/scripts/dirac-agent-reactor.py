#!/usr/bin/env python
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/scripts/dirac-agent-reactor.py,v 1.1 2008/11/14 16:20:50 acasajus Exp $
# File :   dirac-agent
# Author : Adria Casajus, Andrei Tsaregorodtsev, Stuart Paterson
########################################################################
__RCSID__   = "$Id: dirac-agent-reactor.py,v 1.1 2008/11/14 16:20:50 acasajus Exp $"
__VERSION__ = "$Revision: 1.1 $"

"""  This is a script to launch DIRAC agents
"""

import sys
from DIRACEnvironment import DIRAC
from DIRAC.ConfigurationSystem.Client.LocalConfiguration import LocalConfiguration
from DIRAC import gLogger, gConfig
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

ar = AgentReactor()
result = ar.loadAgentModules( positionalArgs )
if not result[ 'OK' ]:
  gLogger.error( "Error while loading agent module: %s" % result[ 'Message' ] )
  sys.exit(1)

