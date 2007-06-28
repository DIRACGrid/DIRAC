# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/DISET/Server.py,v 1.14 2007/06/28 10:40:52 acasajus Exp $
__RCSID__ = "$Id: Server.py,v 1.14 2007/06/28 10:40:52 acasajus Exp $"

import socket
import sys
import select
from DIRAC.Core.DISET.private.Protocols import gProtocolDict
from DIRAC.Core.DISET.private.Dispatcher import Dispatcher
from DIRAC.Core.DISET.private.ServiceConfiguration import ServiceConfiguration
from DIRAC.Core.Utilities.ThreadPool import ThreadPool
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR
from DIRAC.Core.Utilities import Network
from DIRAC.LoggingSystem.Client.Logger import gLogger
from DIRAC.MonitoringSystem.Client.MonitoringClient import gMonitor

class Server:

  bAllowReuseAddress = True
  iListenQueueSize = 5

  def __init__( self, serviceName ):
    gLogger.debug( "Starting service %s" % serviceName )
    while serviceName[0] == "/":
      serviceName = serviceName[1:]
    self.serviceName = serviceName
    self.serviceCfg = ServiceConfiguration( serviceName )
    self.__buildURL()
    self.servicesList = [ self.serviceCfg ]
    self.handlerManager = Dispatcher( self.servicesList )
    retDict = self.handlerManager.loadHandlers()
    if not retDict[ 'OK' ]:
      gLogger.fatal( "Error while loading handler", retDict[ 'Message' ] )
      sys.exit(1)
    self.handlerManager.initializeHandlers()
    self.__initializeTransport()
    self.__initializeMonitor()
    self.threadPool = ThreadPool( 1, self.serviceCfg.getMaxThreads() )
    self.threadPool.daemonize()

  def __initializeMonitor( self ):
    gMonitor.setComponentType( gMonitor.COMPONENT_SERVICE )
    gMonitor.setComponentName( self.serviceName )
    gMonitor.setComponentLocation( self.serviceURL )
    gMonitor.registerActivity( "Queries", "framework", "queries/s", gMonitor.OP_MEAN, 1 )

  def __buildURL( self ):
    protocol = self.serviceCfg.getProtocol()
    self.serviceURL = self.serviceCfg.getURL()
    if self.serviceURL:
        if self.serviceURL.find( protocol ) != 0:
          urlFields = self.serviceURL.split( ":" )
          urlFields[0] = protocol
          self.serviceURL = ":".join( urlFields )
          self.serviceCfg.setURL( self.serviceURL )
        return
    hostName = self.serviceCfg.getHostname()
    port = self.serviceCfg.getPort()
    sURL = "%s://%s:%s/%s" % ( protocol,
                                  hostName,
                                  port,
                                  self.serviceCfg.getName() )
    if sURL[-1] == "/":
      sURL = sURL[:-1]
    self.serviceURL = sURL
    self.serviceCfg.setURL( sURL )

  def __initializeTransport( self ):
    protocol = self.serviceCfg.getProtocol()
    if protocol in gProtocolDict.keys():
      gLogger.verbose( "Initializing %s transport" % protocol, self.serviceURL )
      from DIRAC.Core.DISET.private.Transports.PlainTransport import PlainTransport
      self.transport = gProtocolDict[ protocol ][0]( ( "", self.serviceCfg.getPort() ),
                            bServerMode = True )
      self.transport.initAsServer()
    else:
      gLogger.fatal( "No valid protocol specified for the service", "%s is not a valid protocol" % sProtocol )
      sys.exit(1)

  def serve( self ):
    gLogger.info( "Handler up", "Serving from %s" % self.serviceURL )
    while True:
      gLogger.debug( "Active thread jobs %s" % self.threadPool.numWorkingThreads() )
      self.__handleRequest()
      #self.threadPool.processResults()

  def __handleRequest( self ):
    try:
      inList = [ self.transport.getSocket() ]
      inList, outList, exList = select.select( inList, [], [], 10 )
      if len( inList ) == 1:
        clientTransport = self.transport.acceptConnection()
      else:
        gLogger.debug( "Restart accepting new connections", "" )
        return
    except socket.error:
      return
    if self.__checkClientAddress( clientTransport ):
      self.threadPool.generateJobAndQueueIt( self.processClient,
                                      args = ( clientTransport, ),
                                      oExceptionCallback = self.processClientException )


  def __checkClientAddress( self, clientTransport ):
    #TODO: Check that the IP is not banned
    return True

  def processClientException( self, threadedJob, exceptionInfo ):
    gLogger.exception( "Exception in thread", lException = exceptionInfo )


  def __authorizeProposal( self, service, actionTuple, clientTransport ):
    serviceInfoDict = self.handlerManager.getServiceInfo( service )
    if not serviceInfoDict:
      clientTransport.sendData( S_ERROR( "No handler registered for %s" % service ) )
      return False
    if actionTuple[0] == 'RPC':
      action = actionTuple[1]
    else:
      action = "%s/%s" % actionTuple
    credDict = clientTransport.getConnectingCredentials()
    gLogger.debug( "Trying credentials %s" % credDict )
    if not serviceInfoDict[ 'authManager' ].authQuery( action, credDict ):
      if 'username' in credDict.keys():
        username = credDict[ 'username' ]
      else:
        username = 'unauthenticated'
      gLogger.verbose( "Unauthorized query", "%s by %s" % ( action, username ) )
      clientTransport.sendData( S_ERROR( "Unauthorized query to %s:%s" % ( service, action ) ) )
      return False
    return True

  def processClient( self, clientTransport ):
    try:
      gMonitor.addMark( "Queries" )
      clientTransport.handshake()
      retVal = clientTransport.receiveData( 1024 )
      if not retVal[ 'OK' ]:
        gLogger.error( "Invalid action proposal", retVal[ 'Message' ] )
        return
      proposalTuple = retVal[ 'Value' ]
      gLogger.debug( "Received action from client", str( proposalTuple ) )
      clientTransport.setDisetGroup( proposalTuple[2] )
      requestedService = proposalTuple[0][0]
      if not self.__authorizeProposal( requestedService, proposalTuple[1], clientTransport ):
        return
      handlerDict = self.handlerManager.getHandlerInfo( requestedService )
      try:
        self.handlerManager.lock( requestedService )
        self.__executeAction( proposalTuple, handlerDict, clientTransport )
      finally:
        self.handlerManager.unlock( requestedService )
        pass
    finally:
      clientTransport.close()


  def __executeAction( self, proposalTuple, handlerDict, clientTransport ):
    try:
      serviceInfoDict = self.handlerManager.getServiceInfo( proposalTuple[0][0] )
      serviceInfoDict[ 'instance' ] = proposalTuple[0][1]
      handlerInstance = handlerDict[ "handlerClass" ]( serviceInfoDict,
                      clientTransport,
                      handlerDict[ "lockManager" ] )
      handlerInstance.initialize()
    except Exception, e:
      clientTransport.sendData( S_ERROR( "Server error while initializing handler: %s" % str(e) ) )
      raise
    clientTransport.sendData( S_OK() )
    try:
      handlerInstance.executeAction( proposalTuple[1] )
    except Exception, e:
      gLogger.exception( "Exception while executing handler action" )
      clientTransport.sendData( S_ERROR( "Server error while executing action: %s" % str( e ) ) )


if __name__=="__main__":
  oServer = Server( "Configuration")
  oServer.serve()
