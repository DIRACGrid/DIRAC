# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/Base/Script.py,v 1.3 2007/05/29 16:23:19 acasajus Exp $
__RCSID__ = "$Id: Script.py,v 1.3 2007/05/29 16:23:19 acasajus Exp $"

import sys
import os.path
from DIRAC.ConfigurationSystem.Client.LocalConfiguration import LocalConfiguration
from DIRAC.LoggingSystem.Client.Logger import gLogger

localCfg = LocalConfiguration()

def parseCommandLine( scriptName = False, ignoreErrors = False ):
  global localCfg

  if not scriptName:
    scriptName = os.path.basename( sys.argv[0] )
  scriptSection = localCfg.setConfigurationForScript( scriptName )
  localCfg.addMandatoryEntry( "/DIRAC/Setup" )
  resultDict = localCfg.loadUserData()

  if not ignoreErrors and not resultDict[ 'OK' ]:
    gLogger.error( "There were errors when loading configuration", resultDict[ 'Message' ] )
    sys.exit(1)

def registerSwitch( showKey, longKey, helpString, callback = False ):
  global localCfg
  localCfg.registerCmdOpt( showKey, longKey, helpString, callback )

def getPositionalArgs():
  global localCfg
  return localCfg.getPositionalArguments()

def getUnprocessedSwitches():
  global localCfg
  return localCfg.getUnprocessedSwitches()
