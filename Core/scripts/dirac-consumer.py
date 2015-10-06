#!/usr/bin/env python
"""  This is a script to launch DIRAC consumers
"""

import sys
from DIRAC.ConfigurationSystem.Client.LocalConfiguration import LocalConfiguration
from DIRAC import gLogger
from DIRAC.Core.Base.ConsumerReactor import ConsumerReactor

localCfg = LocalConfiguration()

positionalArgs = localCfg.getPositionalArguments()
if len( positionalArgs ) == 0:
  gLogger.fatal( "You must specify which consumer to run!" )
  sys.exit( 1 )

consumerName = positionalArgs[0]
resultDict = localCfg.loadUserData()
if not resultDict[ 'OK' ]:
  gLogger.error( "There were errors when loading configuration", resultDict[ 'Message' ] )
  sys.exit( 1 )

if len( positionalArgs ) == 1:
  mainName = positionalArgs[0]
else:
  gLogger.error( "More than one consumer name given", resultDict[ 'Message' ] )
  sys.exit( 1 )

consumerReactor = ConsumerReactor( )
#result = consumerReactor.loadAgentModules( positionalArgs )
#if result[ 'OK' ]:
  #consumerReactor.go()
#else:
  #gLogger.error( "Error while loading consumer module", result[ 'Message' ] )
