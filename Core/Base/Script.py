""" This is the guy that you should use when you develop a script that interacts with DIRAC

    And don't forget to call parseCommandLine()
"""

import sys
import os.path
from DIRAC.ConfigurationSystem.Client.LocalConfiguration import LocalConfiguration
from DIRAC.FrameworkSystem.Client.Logger import gLogger
from DIRAC.FrameworkSystem.Client.MonitoringClient import gMonitor
from DIRAC.Core.Utilities.DErrno import includeExtensionErrors

__RCSID__ = "$Id$"

localCfg = LocalConfiguration()

scriptName = os.path.basename( sys.argv[0] ).replace( '.py', '' )

gIsAlreadyInitialized = False

def parseCommandLine( script = False, ignoreErrors = False, initializeMonitor = False ):
  if gIsAlreadyInitialized:
    return False
  gLogger.showHeaders( False )
  return initialize( script, ignoreErrors, initializeMonitor, True )

def initialize( script = False, ignoreErrors = False, initializeMonitor = False, enableCommandLine = False ):
  global scriptName, gIsAlreadyInitialized

  #Please do not call initialize in every file
  if gIsAlreadyInitialized:
    return False
  gIsAlreadyInitialized = True

  userDisabled = not localCfg.isCSEnabled()
  if not userDisabled:
    localCfg.disableCS()

  if not enableCommandLine:
    localCfg.disableParsingCommandLine()

  if script:
    scriptName = script
  localCfg.setConfigurationForScript( scriptName )

  if not ignoreErrors:
    localCfg.addMandatoryEntry( "/DIRAC/Setup" )
  resultDict = localCfg.loadUserData()
  if not ignoreErrors and not resultDict[ 'OK' ]:
    gLogger.error( "There were errors when loading configuration", resultDict[ 'Message' ] )
    sys.exit( 1 )

  if not userDisabled:
    localCfg.enableCS()

  if initializeMonitor:
    gMonitor.setComponentType( gMonitor.COMPONENT_SCRIPT )
    gMonitor.setComponentName( scriptName )
    gMonitor.setComponentLocation( "script" )
    gMonitor.initialize()
  else:
    gMonitor.disable()
  includeExtensionErrors()

  return True

def registerSwitch( showKey, longKey, helpString, callback = False ):
  localCfg.registerCmdOpt( showKey, longKey, helpString, callback )

def getPositionalArgs():
  return localCfg.getPositionalArguments()

def getExtraCLICFGFiles():
  return localCfg.getExtraCLICFGFiles()

def getUnprocessedSwitches():
  return localCfg.getUnprocessedSwitches()

def addDefaultOptionValue( option, value ):
  localCfg.addDefaultEntry( option, value )

def setUsageMessage( usageMessage ):
  localCfg.setUsageMessage( usageMessage )

def disableCS():
  localCfg.disableCS()

def enableCS():
  return localCfg.enableCS()

def showHelp( text = False ):
  localCfg.showHelp( text )
