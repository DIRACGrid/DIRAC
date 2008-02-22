# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/DISET/Server.py,v 1.22 2008/02/22 10:18:49 acasajus Exp $
__RCSID__ = "$Id: Server.py,v 1.22 2008/02/22 10:18:49 acasajus Exp $"

import socket
import sys
import select
from DIRAC.Core.DISET.private.Protocols import gProtocolDict
from DIRAC.Core.DISET.private.Dispatcher import Dispatcher
from DIRAC.Core.DISET.private.GatewayDispatcher import GatewayDispatcher
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
    """
    Constructor

    @type serviceName: string
    @param serviceName: Name of the starting service
    """
    gLogger.debug( "Starting service %s" % serviceName )
    while serviceName[0] == "/":
      serviceName = serviceName[1:]
    self.serviceName = serviceName
    serviceCfg = ServiceConfiguration( serviceName )
    self.__buildURL( serviceCfg )
    self.__initializeMonitor( serviceCfg )
    self.servicesList = [ serviceCfg ]
    if serviceName == GatewayDispatcher.gatewayServiceName:
      self.handlerManager = GatewayDispatcher( self.servicesList )
    else:
      self.handlerManager = Dispatcher( self.servicesList )
    retDict = self.handlerManager.loadHandlers()
    if not retDict[ 'OK' ]:
      gLogger.fatal( "Error while loading handler", retDict[ 'Message' ] )
      sys.exit(1)
    self.handlerManager.initializeHandlers()
    maxThreads = 0
    for serviceCfg in self.servicesList:
      self.__initializeTransport( serviceCfg )
      maxThreads = max( maxThreads, serviceCfg.getMaxThreads() )
    self.threadPool = ThreadPool( 1, maxThreads )
    self.threadPool.daemonize()

  def __initializeMonitor( self, serviceCfg ):
    gMonitor.setComponentType( gMonitor.COMPONENT_SERVICE )
    gMonitor.setComponentName( serviceCfg.getName() )
    gMonitor.setComponentLocation( serviceCfg.getURL() )
    gMonitor.initialize()
    gMonitor.registerActivity( "Queries", "Queries served", "Framework", "queries", gMonitor.OP_SUM )

  def __buildURL( self, serviceCfg ):
    """
    Build the service URL
    """
    protocol = serviceCfg.getProtocol()
    serviceURL = serviceCfg.getURL()
    if serviceURL:
        if serviceURL.find( protocol ) != 0:
          urlFields = serviceURL.split( ":" )
          urlFields[0] = protocol
          self.serviceURL = ":".join( urlFields )
          serviceCfg.setURL( serviceURL )
        return
    hostName = serviceCfg.getHostname()
    port = serviceCfg.getPort()
    sURL = "%s://%s:%s/%s" % ( protocol,
                                  hostName,
                                  port,
                                  serviceCfg.getName() )
    if sURL[-1] == "/":
      sURL = sURL[:-1]
    serviceCfg.setURL( sURL )

  def __initializeTransport( self, serviceCfg ):
    """
    Initialize the transport
    """
    protocol = serviceCfg.getProtocol()
    if protocol in gProtocolDict.keys():
      gLogger.verbose( "Initializing %s transport" % protocol, serviceCfg.getURL() )
      from DIRAC.Core.DISET.private.Transports.PlainTransport import PlainTransport
      self.transport = gProtocolDict[ protocol ][0]( ( "", serviceCfg.getPort() ),
                            bServerMode = True )
      self.transport.initAsServer()
    else:
      gLogger.fatal( "No valid protocol specified for the service", "%s is not a valid protocol" % sProtocol )
      sys.exit(1)

  def serve( self ):
    """
    Start serving petitions
    """
    for serviceCfg in self.servicesList:
      gLogger.info( "Handler up", "Serving from %s" % serviceCfg.getURL() )
    while True:
      self.__handleRequest()
      #self.threadPool.processResults()

  def __handleRequest( self ):
    """
    Handle an incoming request
    """
    try:
      inList = [ self.transport.getSocket() ]
      inList, outList, exList = select.select( inList, [], [], 10 )
      if len( inList ) == 1:
        clientTransport = self.transport.acceptConnection()
      else:
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
    """
    Process an exception generated in a petition
    """
    gLogger.exception( "Exception in thread", lException = exceptionInfo )


  def __authorizeProposal( self, service, actionTuple, clientTransport ):
    """
    Authorize the action being proposed by the client
    """
    serviceInfoDict = self.handlerManager.getServiceInfo( service )
    if actionTuple[0] == 'RPC':
      action = actionTuple[1]
    else:
      action = "%s/%s" % actionTuple
    credDict = clientTransport.getConnectingCredentials()
    retVal = self.handlerManager.authorizeAction( service, action, credDict )
    if not retVal[ 'OK' ]:
      clientTransport.sendData( retVal )
      return False
    return True

  def processClient( self, clientTransport ):
    """
    Receive an action petition and process it from the client
    """
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
      #self.handlerManager.addMark( requestedService )
      if not self.__authorizeProposal( requestedService, proposalTuple[1], clientTransport ):
        return
      try:
        self.handlerManager.lock( requestedService )
        self.__executeAction( proposalTuple, clientTransport )
      finally:
        self.handlerManager.unlock( requestedService )
        pass
    finally:
      clientTransport.close()


  def __executeAction( self, proposalTuple, clientTransport ):
    """
    Execute an action
    """
    try:
      handlerInstance = self.handlerManager.instantiateHandler( proposalTuple[0][0],
                                                                proposalTuple[0][1],
                                                                clientTransport )
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
