#!/usr/bin/env python
"""  This is a script to launch DIRAC consumers.
"""

import sys
from DIRAC.ConfigurationSystem.Client.LocalConfiguration import LocalConfiguration
from DIRAC import gLogger
from DIRAC.Core.Base.ConsumerReactor import ConsumerReactor, loadConsumerModule

def main():
  """It launches DIRAC consumer
  """
  localCfg = LocalConfiguration()
  args = localCfg.getPositionalArguments()
  if not args:
    gLogger.fatal( "You must specify which consumer to run!" )
    sys.exit( 1 )
  if len(args) > 1:
    gLogger.warn( "You added more than one argument, only the first one will be used as consumer name!")
  consumerModuleName = args[0]
  consumerReactor = ConsumerReactor( consumerModuleName )
  result = loadConsumerModule( consumerModuleName )
  if not result[ 'OK' ]:
    gLogger.error( "Error while loading consumer module", result[ 'Message' ] )
    sys.exit( 1 )
  else:
    consumerReactor.consumerModule = result['Value']
  result  = consumerReactor.go()
  if not result[ 'OK' ]:
    gLogger.error( "Error while executing  consumer module", result[ 'Message' ] )
    sys.exit( 1 )
  gLogger.notice( "Graceful exit. Bye!" )
  sys.exit(0)

if __name__ == '__main__':
  main()
