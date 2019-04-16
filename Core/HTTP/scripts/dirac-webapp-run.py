#!/usr/bin/env python

import sys
import tornado
from DIRAC import gConfig, S_OK
from DIRAC.Core.Base import Script
from DIRAC.Core.HTTP.Core.App import App
from DIRAC.FrameworkSystem.Client.Logger import gLogger
from DIRAC.ConfigurationSystem.Client.LocalConfiguration import LocalConfiguration


if __name__ == "__main__":

  def disableDevMode( op ):
    gConfig.setOptionValue( "/WebApp/DevelopMode", "False" )
    return S_OK()
  
  def setHandlers( op ):
    localCfg.handlersLoc = op
    gLogger.notice( "SET handlersLoc to ", localCfg.handlersLoc )
    return S_OK()

  localCfg = LocalConfiguration()

  localCfg.handlersLoc = None

  localCfg.setConfigurationForWeb( "WebApp" )
  localCfg.addMandatoryEntry( "/DIRAC/Setup" )
  localCfg.addDefaultEntry( "/DIRAC/Security/UseServerCertificate", "yes" )
  localCfg.addDefaultEntry( "LogLevel", "INFO" )
  localCfg.addDefaultEntry( "LogColor", True )
  localCfg.registerCmdOpt( "p", "production", "Enable production mode", disableDevMode )
  localCfg.registerCmdOpt( "S:", "service_api_path=", "Specify path to handlers, for ex. 'FrameworkSystem.API'", setHandlers )

  result = localCfg.loadUserData()
  if not result[ 'OK' ]:
    gLogger.initialize( "WebApp", "/" )
    gLogger.fatal( "There were errors when loading configuration", result[ 'Message' ] )
    sys.exit( 1 )
  gLogger.notice( "handlersLoc: ", localCfg.handlersLoc )
  app = App( localCfg.handlersLoc )
  result = app.bootstrap()
  if not result[ 'OK' ]:
    gLogger.fatal( result[ 'Message' ] )
    sys.exit( 1 )
  app.run()


