# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/DISET/private/Dispatcher.py,v 1.15 2008/10/14 13:54:22 acasajus Exp $
__RCSID__ = "$Id: Dispatcher.py,v 1.15 2008/10/14 13:54:22 acasajus Exp $"

import DIRAC
from DIRAC.LoggingSystem.Client.Logger import gLogger
from DIRAC.Core.DISET.private.LockManager import LockManager
from DIRAC.Core.Utilities import List, Time
from DIRAC.Core.DISET.AuthManager import AuthManager
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR
from DIRAC.MonitoringSystem.Client.MonitoringClient import MonitoringClient
from DIRAC.FrameworkSystem.Client.SecurityLogClient import SecurityLogClient

class Dispatcher:

  __securityLog = SecurityLogClient()

  def __init__( self, serviceCfgList ):
    self.servicesDict = {}
    for serviceCfg in serviceCfgList:
      self.servicesDict[ serviceCfg.getName() ] = { 'cfg' : serviceCfg }
    self.startTime = Time.dateTime()

  def loadHandlers( self ):
    """
    Load handlers for service
    """
    for serviceName in self.servicesDict.keys():
      serviceCfg = self.servicesDict[ serviceName ][ 'cfg' ]
      retVal = self.__registerHandler( serviceName )
      if not retVal[ 'OK' ]:
        return retVal
      self.__initializeLocks( serviceName )
      self.__generateServiceInfo( serviceName )
    return S_OK()

  def __generateServiceInfo( self, serviceName ):
    """
    Generate the dict with the info of the service
    """
    serviceCfg = self.servicesDict[ serviceName ][ 'cfg' ]
    serviceInfoDict = { 'serviceName' : serviceName,
                        'URL' : serviceCfg.getURL(),
                        'systemSectionPath' : serviceCfg.getSystemPath(),
                        'serviceSectionPath' : serviceCfg.getServicePath(),
                  }
    self.servicesDict[ serviceName ][ 'serviceInfo' ] = serviceInfoDict
    self.servicesDict[ serviceName ][ 'authManager' ] = AuthManager( "%s/Authorization" % serviceCfg.getServicePath() )

  def __initializeLocks( self, serviceName ):
    """
    Initialize all locks for the service
    """
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
    """
    Load a given handler for a service
    """
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

  def _getHandlerInfo( self, serviceName ):
    """
    Get the handler info for a given service
    """
    if serviceName in self.servicesDict.keys():
      gLogger.debug( "Dispatching action", "Found handler for %s" % serviceName )
      return self.servicesDict[ serviceName ][ 'handlerInfo' ]
    return False

  def _getServiceInfo( self, serviceName ):
    """
    Get the service Info
    """
    if serviceName in self.servicesDict:
      return dict( self.servicesDict[ serviceName ][ 'serviceInfo' ] )
    return False

  def initializeHandlers( self ):
    """
    Call the static initialization for all handlers
    """
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

  def _lock( self, serviceName ):
    """
    Lock a service
    """
    self.servicesDict[ serviceName ][ 'lockManager' ].lockGlobal()

  def _unlock( self, serviceName ):
    """
    Unlock a service
    """
    self.servicesDict[ serviceName ][ 'lockManager' ].unlockGlobal()

  def _instantiateHandler( self, serviceName, clientDataDict, clientTransport ):
    """
    Generate an instance of the handler for a given service
    """
    serviceInfoDict = self._getServiceInfo( serviceName )
    handlerDict = self._getHandlerInfo( serviceName )
    for key in clientDataDict:
      serviceInfoDict[ key ] = clientDataDict[ key ]
    handlerInstance = handlerDict[ "handlerClass" ]( serviceInfoDict,
                    clientTransport,
                    handlerDict[ "lockManager" ] )
    handlerInstance.initialize()
    return handlerInstance

  def __formattedRemoteCredentials( self, clientTransport ):
    peerCreds = clientTransport.getConnectingCredentials()
    if peerCreds.has_key( 'username' ):
      peerId = "[%s:%s]" % ( peerCreds[ 'group' ], peerCreds[ 'username' ] )
    else:
      peerId = ""
    addr = clientTransport.getRemoteAddress()
    if addr:
      addr = "%s:%s" % ( addr[0], addr[1] )
    return "(%s)%s" % ( addr, peerId )

  def processClient( self, clientTransport ):
    """
    Client's here! Do stuff!
    """
    retVal = clientTransport.receiveData( 1024 )
    if not retVal[ 'OK' ]:
      gLogger.error( "Invalid action proposal", "%s %s" % ( self.__formattedRemoteCredentials(clientTransport),
                                                            retVal[ 'Message' ] ) )
      return
    proposalTuple = retVal[ 'Value' ]
    gLogger.debug( "Received action from client", str( proposalTuple ) )
    if proposalTuple[2]:
      clientTransport.setExtraCredentials( proposalTuple[2] )
    requestedService = proposalTuple[0][0]
    if not self._authorizeClientProposal( requestedService, proposalTuple[1], clientTransport ):
      return
    try:
      self._lock( requestedService )
      self._executeAction( proposalTuple, clientTransport )
    finally:
      self._unlock( requestedService )

  def _createIdentityStringFromCredDict( self, credDict ):
    if 'username' in credDict:
      if 'group' in credDict:
        identity = "[%s:%s]" % ( credDict[ 'username' ], credDict[ 'group' ]  )
      else:
        identity = "[%s:unknown]" % credDict[ 'username' ]
    else:
      identity = 'unknown'
    if 'DN' in credDict:
      identity += "(%s)" % credDict[ 'DN' ]
    return identity

  def _authorizeClientProposal( self, serviceName, actionTuple, clientTransport ):
    """
    Authorize the action being proposed by the client
    """
    #serviceInfoDict = self._getServiceInfo( service )
    if actionTuple[0] == 'RPC':
      action = actionTuple[1]
    else:
      action = "%s/%s" % actionTuple
    credDict = clientTransport.getConnectingCredentials()
    authorized = True
    retVal = self._authorizeAction( serviceName, action, credDict )
    identity = self._createIdentityStringFromCredDict( credDict )
    if not retVal[ 'OK' ]:
      gLogger.error( "Unauthorized query", "to %s:%s by %s" % ( serviceName, action, identity ) )
      clientTransport.sendData( retVal )
      authorized = False
    #Security log
    sourceAddress = clientTransport.getRemoteAddress()
    self.__securityLog.addMessage( authorized, sourceAddress[0], sourceAddress[1], identity,
                                   self.servicesDict[ serviceName ]['cfg'].getHostname(),
                                   self.servicesDict[ serviceName ]['cfg'].getPort(),
                                   serviceName, "/".join( actionTuple ) )
    #end security log
    return authorized

  def _authorizeAction( self, serviceName, action, credDict ):
    """
    Authorize an action for a given credentials dictionary
    """
    if not serviceName in self.servicesDict:
      return S_ERROR( "No handler registered for %s" % serviceName )
    gLogger.debug( "Trying credentials %s" % credDict )
    if not self.servicesDict[ serviceName ][ 'authManager' ].authQuery( action, credDict ):


      return S_ERROR( "Unauthorized query to %s:%s" % ( serviceName, action ) )
    return S_OK()

  def _executeAction( self, proposalTuple, clientTransport ):
    """
    Execute an action
    """
    clientParams = { 'clientSetup' : proposalTuple[0][1],
                     'serviceStartTime' : self.startTime,
                     'clientAddress' : clientTransport.getRemoteAddress() }
    try:
      handlerInstance = self._instantiateHandler( proposalTuple[0][0],
                                                                clientParams,
                                                                clientTransport )
    except Exception, e:
      clientTransport.sendData( S_ERROR( "Server error while initializing handler: %s" % str(e) ) )
      raise
    retVal = clientTransport.sendData( S_OK() )
    if not retVal[ 'OK' ]:
      return retVal
    try:
      handlerInstance.executeAction( proposalTuple[1] )
    except Exception, e:
      gLogger.exception( "Exception while executing handler action" )
      retVal = clientTransport.sendData( S_ERROR( "Server error while executing action: %s" % str( e ) ) )
      if not retVal[ 'OK' ]:
        return retVal
    return S_OK()