#!/usr/bin/env python
"""  This is a script to launch DIRAC consumers
"""

import sys
from DIRAC.ConfigurationSystem.Client.LocalConfiguration import LocalConfiguration
from DIRAC import gLogger
from DIRAC.Core.Base.ConsumerReactor import ConsumerReactor

def main():
  """It launches DIRAC consumers
  """
  localCfg = LocalConfiguration()
  consumerModuleNames = localCfg.getPositionalArguments()
  if len( consumerModuleNames ) == 0:
    gLogger.fatal( "You must specify which consumer to run!" )
    sys.exit( 1 )
  consumerReactor = ConsumerReactor()
  result = consumerReactor.loadModules( consumerModuleNames )
  if not result[ 'OK' ]:
    gLogger.error( "Error while loading consumer module", result[ 'Message' ] )
    sys.exit( 1 )
  result  = consumerReactor.go()
  if not result[ 'OK' ]:
    gLogger.error( "Error while executing  consumer module", result[ 'Message' ] )
    sys.exit( 1 )
  gLogger.notice( "Graceful exit. Bye!" )
  sys.exit(0)

if __name__ == '__main__':
  main()
