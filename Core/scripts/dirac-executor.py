#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :   dirac-executor
# Author : Adria Casajus
########################################################################
__RCSID__ = "$Id$"

"""  This is a script to launch DIRAC executors
"""

import sys
import DIRAC
from DIRAC.ConfigurationSystem.Client.LocalConfiguration import LocalConfiguration
from DIRAC import gLogger, gConfig
from DIRAC.Core.Base.ExecutorReactor import ExecutorReactor

localCfg = LocalConfiguration()

positionalArgs = localCfg.getPositionalArguments()
if len( positionalArgs ) == 0:
  gLogger.fatal( "You must specify which executor to run!" )
  sys.exit( 1 )

if len( positionalArgs ) == 1 and positionalArgs[0].find( "/" ) > -1:
  mainName = positionalArgs[0]
else:
  mainName = "Framework/MultiExecutor"

localCfg.setConfigurationForExecutor( mainName )
localCfg.addMandatoryEntry( "/DIRAC/Setup" )
localCfg.addDefaultEntry( "/DIRAC/Security/UseServerCertificate", "yes" )
localCfg.addDefaultEntry( "LogLevel", "INFO" )
localCfg.addDefaultEntry( "LogColor", True )
resultDict = localCfg.loadUserData()
if not resultDict[ 'OK' ]:
  gLogger.fatal( "There were errors when loading configuration", resultDict[ 'Message' ] )
  sys.exit( 1 )

executorReactor = ExecutorReactor()

result = executorReactor.loadModules( positionalArgs )
if not result[ 'OK' ]:
  gLogger.fatal( "Error while loading executor", result[ 'Message' ] )
  sys.exit( 1 )

result = executorReactor.go()
if not result[ 'OK' ]:
  gLogger.fatal( result[ 'Message' ] )
  sys.exit( 1 )

gLogger.notice( "Graceful exit. Bye!" )
sys.exit(0)
