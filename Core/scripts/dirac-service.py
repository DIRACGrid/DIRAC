#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :   dirac-service
# Author : Adria Casajus
########################################################################
__RCSID__   = "$Id$"
__VERSION__ = "$Revision: 1.7 $"

import sys
from DIRACEnvironment import DIRAC
from DIRAC.ConfigurationSystem.Client.LocalConfiguration import LocalConfiguration
from DIRAC.FrameworkSystem.Client.Logger import gLogger
from DIRAC.Core.DISET.Server import Server

localCfg = LocalConfiguration()

positionalArgs = localCfg.getPositionalArguments()
if len( positionalArgs ) == 0:
  gLogger.initialize( "NOT SPECIFIED", "/" )
  gLogger.fatal( "You must specify which server to run!" )
  sys.exit(1)

serverName = positionalArgs[0]
localCfg.setConfigurationForServer( serverName )
localCfg.addMandatoryEntry( "Port" )
#localCfg.addMandatoryEntry( "HandlerPath" )
localCfg.addMandatoryEntry( "/DIRAC/Setup" )
localCfg.addDefaultEntry( "/DIRAC/Security/UseServerCertificate", "yes" )
resultDict = localCfg.loadUserData()
if not resultDict[ 'OK' ]:
  gLogger.initialize( serverName, "/" )
  gLogger.error( "There were errors when loading configuration", resultDict[ 'Message' ] )
  sys.exit(1)



serverToLaunch = Server( serverName )
serverToLaunch.serve()
