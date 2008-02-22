# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/DISET/private/Dispatcher.py,v 1.7 2008/02/22 10:18:49 acasajus Exp $
__RCSID__ = "$Id: Dispatcher.py,v 1.7 2008/02/22 10:18:49 acasajus Exp $"

import DIRAC
from DIRAC.LoggingSystem.Client.Logger import gLogger
from DIRAC.Core.DISET.private.LockManager import LockManager
from DIRAC.Core.Utilities import List
from DIRAC.Core.DISET.AuthManager import AuthManager
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR
from DIRAC.MonitoringSystem.Client.MonitoringClient import MonitoringClient

class Dispatcher:

  def __init__( self, serviceCfgList ):
    self.servicesDict = {}
    for serviceCfg in serviceCfgList:
      self.servicesDict[ serviceCfg.getName() ] = { 'cfg' : serviceCfg }

  def __initializeMonitor( self, serviceName ):
    """
    Initialize the system monitor client
    """
    serviceCfg = self.servicesDict[ serviceName ][ 'cfg' ]
    serviceMonitor = MonitoringClient()
    serviceMonitor.setComponentType( serviceMonitor.COMPONENT_SERVICE )
    serviceMonitor.setComponentName( serviceCfg.getName() )
    serviceMonitor.setComponentLocation( serviceCfg.getURL() )
    serviceMonitor.initialize()
    serviceMonitor.registerActivity( "Queries", "Queries served", "Framework", "queries", serviceMonitor.OP_SUM )
    self.servicesDict[ serviceName ][ 'monitorClient' ] = serviceMonitor

  def addMark( self, serviceName ):
    self.servicesDict[ serviceName ][ 'monitorClient' ].addMark( "Queries", 1 )

  def loadHandlers( self ):
    for serviceName in self.servicesDict.keys():
      serviceCfg = self.servicesDict[ serviceName ][ 'cfg' ]
      retVal = self.__registerHandler( serviceName )
      if not retVal[ 'OK' ]:
        return retVal
      self.__initializeLocks( serviceName )
      self.__generateServiceInfo( serviceName )
      #self.__initializeMonitor( serviceName )
    return S_OK()

  def __generateServiceInfo( self, serviceName ):
    serviceCfg = self.servicesDict[ serviceName ][ 'cfg' ]
    serviceInfoDict = { 'serviceName' : serviceName,
                        'URL' : serviceCfg.getURL(),
                        'systemSectionPath' : serviceCfg.getSystemPath(),
                        'serviceSectionPath' : serviceCfg.getServicePath(),
                  }
    self.servicesDict[ serviceName ][ 'serviceInfo' ] = serviceInfoDict
    self.servicesDict[ serviceName ][ 'authManager' ] = AuthManager( "%s/Authorization" % serviceCfg.getServicePath() )

  def __initializeLocks( self, serviceName ):
    serviceCfg = self.servicesDict[ serviceName ][ 'cfg' ]
    handlerDict = self.servicesDict[ serviceName ][ 'handlerInfo' ]
    requestHandler = handlerDict[ "handlerClass" ]

    maxWaitingRequests = serviceCfg.getMaxWaitingPetitions()
    lockManager = LockManager( maxWaitingRequests )
    self.servicesDict[ serviceName ][ 'lockManager' ] = lockManager
    funcLockManager = lockManager.createNewLockManager( serviceName )
    handlerDict[ "lockManager" ] = funcLockManager
    for methodName in dir( requestHandler ):
      if methodName.find( "export_" ) == 0:
        exportedMethodName = methodName.replace( "export_", "" )
        threadLimit = serviceCfg.getMaxThreadsPerFunction( exportedMethodName )
        funcLockManager.createNewLock( exportedMethodName, threadLimit )

  def __registerHandler( self, serviceName ):
    serviceCfg = self.servicesDict[ serviceName ][ 'cfg' ]

    handlerLocation = serviceCfg.getHandlerLocation()
    if not handlerLocation:
      return S_ERROR( "handlerLocation is not defined in %s" % serviceCfg.getSectionPath() )
    gLogger.debug( "Found a handler", handlerLocation )
    if handlerLocation.find( "Handler.py" ) != len( handlerLocation ) - 10:
      return S_ERROR( "File %s does not have a valid handler name" % handlerLocation )
    handlerLocation = handlerLocation.replace( ".py", "" )
    lServicePath = List.fromChar( handlerLocation, "/" )
    handlerName = lServicePath[-1]
    try:
      handlerModule = __import__( ".".join( lServicePath ),
                                   globals(),
                                   locals(), handlerName )
      handlerClass  = getattr( handlerModule, handlerName )
    except Exception, e:
      gLogger.exception()
      return S_ERROR( "Can't import handler: %s" % str( e ) )
    try:
      handlerInitMethod = getattr( handlerModule, "initialize%s" % handlerName )
      gLogger.debug( "Found initialization function for service" )
    except:
      handlerInitMethod = False
      gLogger.debug( "Not found initialization function for service" )
    handlerDict = {}
    handlerDict[ "handlerName" ] = handlerName
    handlerDict[ "handlerModule" ] = handlerModule
    handlerDict[ "handlerClass" ] = handlerClass
    handlerDict[ "handlerInitialization" ] = handlerInitMethod

    self.servicesDict[ serviceName ][ 'handlerInfo' ] = handlerDict
    return S_OK()

  def getHandlerInfo( self, serviceName ):
    if serviceName in self.servicesDict.keys():
      gLogger.debug( "Dispatching action", "Found handler for %s" % serviceName )
      return self.servicesDict[ serviceName ][ 'handlerInfo' ]
    return False

  def getServiceInfo( self, serviceName ):
    if serviceName in self.servicesDict:
      return dict( self.servicesDict[ serviceName ][ 'serviceInfo' ] )
    return False

  def initializeHandlers( self ):
    for serviceName in self.servicesDict.keys():
      serviceInfo = self.servicesDict[ serviceName ][ 'serviceInfo' ]
      handlerDict = self.servicesDict[ serviceName ][ 'handlerInfo' ]
      handlerInitFunc = handlerDict[ "handlerInitialization" ]
      if handlerInitFunc:
        try:
          retDict = handlerInitFunc( serviceInfo  )
        except Exception, e:
          gLogger.exception()
          DIRAC.abort( 10, "Can't call handler initialization function" "for service %s", ( serviceName, str(e) ) )
        if not retDict[ 'OK' ]:
          DIRAC.abort( 10, "Error in the initialization function", retDict[ 'Message' ] )

  def lock( self, serviceName ):
    self.servicesDict[ serviceName ][ 'lockManager' ].lockGlobal()

  def unlock( self, serviceName ):
    self.servicesDict[ serviceName ][ 'lockManager' ].unlockGlobal()

  def authorizeAction( self, serviceName, action, credDict ):
    if not serviceName in self.servicesDict:
      return S_ERROR( "No handler registered for %s" % serviceName )
    gLogger.debug( "Trying credentials %s" % credDict )
    if not self.servicesDict[ serviceName ][ 'authManager' ].authQuery( action, credDict ):
      if 'username' in credDict.keys():
        username = credDict[ 'username' ]
      else:
        username = 'unauthenticated'
      gLogger.verbose( "Unauthorized query", "%s by %s" % ( action, username ) )
      return S_ERROR( "Unauthorized query to %s:%s" % ( service, action ) )
    return S_OK()

  def instantiateHandler( self, serviceName, clientSetup, clientTransport ):
    """
    Execute an action
    """
    serviceInfoDict = self.getServiceInfo( serviceName )
    handlerDict = self.getHandlerInfo( serviceName )
    serviceInfoDict[ 'clientSetup' ] = clientSetup
    handlerInstance = handlerDict[ "handlerClass" ]( serviceInfoDict,
                    clientTransport,
                    handlerDict[ "lockManager" ] )
    handlerInstance.initialize()
    return handlerInstance