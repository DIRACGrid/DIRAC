# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/DISET/private/GatewayDispatcher.py,v 1.1 2008/02/22 10:18:49 acasajus Exp $
__RCSID__ = "$Id: GatewayDispatcher.py,v 1.1 2008/02/22 10:18:49 acasajus Exp $"

import DIRAC
from DIRAC.LoggingSystem.Client.Logger import gLogger
from DIRAC.Core.DISET.AuthManager import AuthManager
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR
from DIRAC.Core.DISET.private.Dispatcher import Dispatcher
from DIRAC.ConfigurationSystem.Client.PathFinder import getServiceSection

class GatewayDispatcher( Dispatcher ):

  gatewayServiceName = "Framework/Gateway"

  def __init__( self, serviceCfgList ):
    self.servicesDict = {}
    for serviceCfg in serviceCfgList:
      self.servicesDict[ serviceCfg.getName() ] = { 'cfg' : serviceCfg }

  def getHandlerInfo( self, serviceName ):
    return self.servicesDict[ self.gatewayServiceName ][ 'handlerInfo' ]

  def getServiceInfo( self, serviceName ):
    return dict( self.servicesDict[ self.gatewayServiceName ][ 'serviceInfo' ] )

  def lock( self, serviceName ):
    self.servicesDict[ self.gatewayServiceName ][ 'lockManager' ].lockGlobal()

  def unlock( self, serviceName ):
    self.servicesDict[ self.gatewayServiceName ][ 'lockManager' ].unlockGlobal()

  def authorizeAction( self, serviceName, action, credDict ):
    try:
      authManager = AuthManager( "%s/Authorization" % getServiceSection( serviceName ) )
    except:
      return S_ERROR( "Service %s is unknown" % serviceName )
    gLogger.debug( "Trying credentials %s" % credDict )
    if not authManager.authQuery( action, credDict ):
      if 'username' in credDict.keys():
        username = credDict[ 'username' ]
      else:
        username = 'unauthenticated'
      gLogger.info( "Unauthorized query", "%s by %s" % ( action, username ) )
      return S_ERROR( "Unauthorized query to %s:%s" % ( service, action ) )
    return S_OK()

  def instantiateHandler( self, serviceName, clientSetup, clientTransport ):
    """
    Execute an action
    """
    serviceInfoDict = self.getServiceInfo( serviceName )
    handlerDict = self.getHandlerInfo( serviceName )
    serviceInfoDict[ 'clientSetup' ] = clientSetup
    serviceInfoDict[ 'serviceName' ] = serviceName
    handlerInstance = handlerDict[ "handlerClass" ]( serviceInfoDict,
                    clientTransport,
                    handlerDict[ "lockManager" ] )
    handlerInstance.initialize()
    return handlerInstance