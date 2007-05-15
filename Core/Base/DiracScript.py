# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/Base/Attic/DiracScript.py,v 1.2 2007/05/15 14:48:28 acasajus Exp $
__RCSID__ = "$Id: DiracScript.py,v 1.2 2007/05/15 14:48:28 acasajus Exp $"

import sys
import os.path
from DIRAC.ConfigurationSystem.Client.LocalConfiguration import LocalConfiguration
from DIRAC.LoggingSystem.Client.Logger import gLogger

localCfg = LocalConfiguration()

positionalArgs = localCfg.getPositionalArguments()
scriptName = os.path.basename( sys.argv[0] )
scriptSection = localCfg.setConfigurationForScript( scriptName )
localCfg.addMandatoryEntry( "/DIRAC/Setup" )
resultDict = localCfg.loadUserData()
if not resultDict[ 'OK' ]:
  gLogger.error( "There were errors when loading configuration", resultDict[ 'Message' ] )
  sys.exit(1)
