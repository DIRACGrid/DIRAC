#!/usr/bin/env python
########################################################################
# File :   dirac-service
# Author : Adria Casajus
########################################################################
__RCSID__ = "$Id$"

import sys
import signal 

from DIRAC.ConfigurationSystem.Client.LocalConfiguration import LocalConfiguration
from DIRAC.FrameworkSystem.Client.Logger import gLogger
from DIRAC.Core.DISET.ServiceReactor import ServiceReactor
from DIRAC.Core.Utilities.DErrno import includeExtensionErrors

localCfg = LocalConfiguration()

def stopChildProcesses( _sig, frame ):
    """
    It is used to properly stop tornado when more than one process is used.
    In principle this is doing the job of runsv....
    :param int sig: the signal sent to the process
    :param object frame: execution frame which contains the child processes
    """
    print 'AAAAAAAA!!!!', frame.f_locals
    handler = frame.f_locals.get('self')
    if handler and isinstance(handler,ServiceReactor):
      handler.stopAllProcess()
    
    for child in frame.f_locals.get( 'children', [] ):
      gLogger.info( "Stopping child processes: %d" % child )
      os.kill( child, signal.SIGTERM )
    
    #sys.exit( 0 )
    
    
positionalArgs = localCfg.getPositionalArguments()
if len( positionalArgs ) == 0:
  gLogger.fatal( "You must specify which server to run!" )
  sys.exit( 1 )

serverName = positionalArgs[0]
localCfg.setConfigurationForServer( serverName )
localCfg.addMandatoryEntry( "Port" )
#localCfg.addMandatoryEntry( "HandlerPath" )
localCfg.addMandatoryEntry( "/DIRAC/Setup" )
localCfg.addDefaultEntry( "/DIRAC/Security/UseServerCertificate", "yes" )
localCfg.addDefaultEntry( "LogLevel", "INFO" )
localCfg.addDefaultEntry( "LogColor", True )
resultDict = localCfg.loadUserData()
if not resultDict[ 'OK' ]:
  gLogger.initialize( serverName, "/" )
  gLogger.error( "There were errors when loading configuration", resultDict[ 'Message' ] )
  sys.exit( 1 )

includeExtensionErrors()


serverToLaunch = ServiceReactor()
result = serverToLaunch.initialize( positionalArgs )
if not result[ 'OK' ]:
  gLogger.error( result[ 'Message' ] )
  sys.exit( 1 )
#signal.signal(signal.SIGTERM, stopChildProcesses)
#signal.signal(signal.SIGINT, stopChildProcesses)
result = serverToLaunch.serve()
if not result[ 'OK' ]:
  gLogger.error( result[ 'Message' ] )
  sys.exit( 1 )
