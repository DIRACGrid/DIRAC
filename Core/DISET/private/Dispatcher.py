# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/DISET/private/Dispatcher.py,v 1.6 2007/06/13 19:29:39 acasajus Exp $
__RCSID__ = "$Id: Dispatcher.py,v 1.6 2007/06/13 19:29:39 acasajus Exp $"

import DIRAC
from DIRAC.LoggingSystem.Client.Logger import gLogger
from DIRAC.Core.DISET.private.LockManager import LockManager
from DIRAC.Core.Utilities import List
from DIRAC.Core.DISET.AuthManager import AuthManager
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR

class Dispatcher:

  def __init__( self, serviceCfgList ):
    self.servicesDict = {}
    for serviceCfg in serviceCfgList:
      self.servicesDict[ serviceCfg.getName() ] = { 'cfg' : serviceCfg }

  def loadHandlers( self ):
    for serviceName in self.servicesDict.keys():
      serviceCfg = self.servicesDict[ serviceName ][ 'cfg' ]
      retVal = self.__registerHandler( serviceName )
      if not retVal[ 'OK' ]:
        return retVal
      self.__initializeLocks( serviceName )
      self.__generateServiceInfo( serviceName )
    return S_OK()

  def __generateServiceInfo( self, serviceName ):
    serviceCfg = self.servicesDict[ serviceName ][ 'cfg' ]
    serviceInfoDict = { 'serviceName' : serviceName,
                        'URL' : serviceCfg.getURL(),
                        'systemSectionPath' : serviceCfg.getSystemPath(),
                        'serviceSectionPath' : serviceCfg.getServicePath(),
                        'authManager' : AuthManager( "%s/Authorization" % serviceCfg.getServicePath() )
                  }
    self.servicesDict[ serviceName ][ 'serviceInfo' ] = serviceInfoDict

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
      return self.servicesDict[ serviceName ][ 'serviceInfo' ]
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
