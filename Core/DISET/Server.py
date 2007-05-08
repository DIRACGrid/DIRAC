# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/DISET/Server.py,v 1.7 2007/05/08 17:09:11 acasajus Exp $
__RCSID__ = "$Id: Server.py,v 1.7 2007/05/08 17:09:11 acasajus Exp $"

import socket
import sys
from DIRAC.Core.DISET.private.Protocols import gProtocolDict
from DIRAC.Core.DISET.private.Dispatcher import Dispatcher
from DIRAC.Core.DISET.private.ServiceConfiguration import ServiceConfiguration
from DIRAC.Core.Utilities.ThreadPool import ThreadPool
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR
from DIRAC.Core.Utilities import Network
from DIRAC.LoggingSystem.Client.Logger import gLogger
from DIRAC.ActivitySystem.Client.ActivityClient import gActivity
from DIRAC.ActivitySystem.Client.Constants import *

class Server:

  bAllowReuseAddress = True
  iListenQueueSize = 5

  def __init__( self, serviceName ):
    gLogger.debug( "Starting service %s" % serviceName )
    while serviceName[0] == "/":
      serviceName = serviceName[1:]
    self.serviceName = serviceName
    self.serviceCfg = ServiceConfiguration( serviceName )
    self.serviceAddress = ( "", self.serviceCfg.getPort() )
    self.__buildURL()
    self.handlerManager = Dispatcher( [ self.serviceCfg ] )
    retDict = self.handlerManager.loadHandlers()
    if not retDict[ 'OK' ]:
      gLogger.fatal( "Error while loading handler", retDict[ 'Message' ] )
      sys.exit(1)
    self.handlerManager.initializeHandlers()
    self.__initializeTransport()
    self.__initializeActivity()
    self.threadPool = ThreadPool( 1, self.serviceCfg.getMaxThreads() )
    self.threadPool.daemonize()

  def __initializeActivity( self ):
    gActivity.setComponentType( ACTIVITY_COMPONENT_SERVICE )
    gActivity.setComponentName( self.serviceName )
    gActivity.setComponentLocation( self.serviceURL )
    gActivity.registerActivity( "Queries", "framework", "queries/s", ACTIVITY_OP_MEAN, 1 )

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
    port = self.serviceAddress[1]
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
      gLogger.verbose( "Initializing %s transport" % protocol, str( self.serviceAddress ) )
      from DIRAC.Core.DISET.private.Transports.PlainTransport import PlainTransport
      self.transport = gProtocolDict[ protocol ]( self.serviceAddress, bServerMode = True )
      self.transport.initAsServer()
    else:
      gLogger.fatal( "No valid protocol specified for the service", "%s is not a valid protocol" % sProtocol )
      sys.exit(1)

  def serve( self ):
    gLogger.info( "Handler up", "Serving from %s" % self.serviceURL )
    while True:
      self.__handleRequest()
      #self.threadPool.processResults()

  def __handleRequest( self ):
    try:
      clientTransport = self.transport.acceptConnection()
    except socket.error:
      return
    if self.__checkClientAddress( clientTransport ):
      gActivity.addMark( "Queries" )
      self.threadPool.generateJobAndQueueIt( self.processClient,
                                      args = ( clientTransport, ),
                                      oExceptionCallback = self.processClientException )


  def __checkClientAddress( self, clientTransport ):
    #TODO: Check that the IP is not banned
    return True

  def processClientException( self, threadedJob, exceptionInfo ):
    gLogger.exception( "Exception in thread", lException = exceptionInfo )

  def processClient( self, clientTransport ):
    clientTransport.handshake()
    actionTuple = clientTransport.receiveData( 1024 )
    gLogger.debug( "Received action from client", str( actionTuple ) )
    wantedService = actionTuple[0][0]
    handlerDict = self.handlerManager.getHandlerInfo( wantedService )
    if not handlerDict:
      clientTransport.sendData( S_ERROR( "No handler registered for %s" % wantedService ) )
      clientTransport.close()
      return
    try:
      self.handlerManager.lock( wantedService )
      self.__executeAction( actionTuple, clientTransport)
    finally:
      self.handlerManager.unlock( wantedService )

  def __executeAction( self, actionTuple, clientTransport ):
    handlerDict = self.handlerManager.getHandlerInfo( actionTuple[0][0] )
    try:
      serviceInfoDict = self.handlerManager.getServiceInfo( actionTuple[0][0] )
      serviceInfoDict[ 'instance' ] = actionTuple[0][1]
      handlerInstance = handlerDict[ "handlerClass" ]( serviceInfoDict,
                      clientTransport,
                      handlerDict[ "lockManager" ] )
      handlerInstance.initialize()
    except Exception, e:
      clientTransport.sendData( S_ERROR( "Cannot process request; %s" % str(e) ) )
      raise
    clientTransport.sendData( S_OK() )
    try:
      handlerInstance.executeAction( actionTuple[1] )
    except Exception, e:
      gLogger.exception( "Exception while executing handler action" )
      clientTransport.sendData( S_ERROR( "Exception while executing action: %s" % str( e ) ) )
    clientTransport.close()


if __name__=="__main__":
  oServer = Server( "Configuration")
  oServer.serve()
