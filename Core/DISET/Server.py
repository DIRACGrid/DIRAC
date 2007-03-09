# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/DISET/Server.py,v 1.1 2007/03/09 15:27:49 rgracian Exp $
__RCSID__ = "$Id: Server.py,v 1.1 2007/03/09 15:27:49 rgracian Exp $"

import socket
import sys
from DIRAC.Core.DISET.private.Dispatcher import Dispatcher
from DIRAC.Core.Utilities.ThreadPool import ThreadPool
from DIRAC.Core.DISET.private.ServiceConfiguration import ServiceConfiguration
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR
from DIRAC.Core.Utilities import Network
from DIRAC.LoggingSystem.Client.Logger import gLogger

class Server:
    
  bAllowReuseAddress = True
  iListenQueueSize = 5

  def __init__( self, sService ):
    gLogger.debug( "Starting service %s" % sService )
    while sService[0] == "/":
      sService = sService[1:]
    self.sService = sService
    self.oServiceConf = ServiceConfiguration( sService )
    self.oDispatcher = Dispatcher( self.oServiceConf )
    dRetVal = self.oDispatcher.loadHandler()
    if not dRetVal[ 'OK' ]:
      gLogger.fatal( "Error while loading handler", dRetVal[ 'Message' ] )
      sys.exit(1) 
    self.oThreadPool = ThreadPool( 1, self.oServiceConf.getMaxThreads() )
    self.oThreadPool.daemonize()
    self.stServerAddress = self.__getServiceAddress()
    self.__buildURL()
    self.__initializeHandler()
    self.__initializeTransport()
    
  def __getProtocol( self ):
    #TODO: Return the appropiate protocol
    return "dit"
  
  def __buildURL( self ):
    self.sServiceURL = self.oServiceConf.getURL()
    if self.sServiceURL:
        return
    stServiceAddress = self.__getServiceAddress()
    sHost = stServiceAddress[0]
    iPort = stServiceAddress[1]
    if sHost == "":
      sHost = Network.getFQDN()
    sServiceName = self.oDispatcher.getServiceName()
    sURL = "%s://%s:%s/%s" % ( self.__getProtocol(),
                                  sHost,
                                  iPort,
                                  sServiceName )
    if sURL[-1] == "/":
      sURL = sURL[:-1]
    self.sServiceURL = sURL
    self.oServiceConf.setURL( sURL )
        
  def __initializeTransport( self ):
    sProtocol = self.__getProtocol()
    if "dit" == sProtocol:
      gLogger.debug( "Initializing Plain Transport", str( self.stServerAddress ) )
      from DIRAC.Core.DISET.private.Transports.PlainTransport import PlainTransport
      self.oTransport = PlainTransport( self.stServerAddress, bServerMode = True )
      self.oTransport.initAsServer()
    else:
      gLogger.fatal( "No valid protocol specified for the service", "%s is not a valid protocol" % sProtocol )
      sys.exit(1)
    
  def __initializeHandler( self ):
    dHandler = self.oDispatcher.getHandlerInfo()
    oHandlerInitFunction = dHandler[ "handlerInitialization" ]
    if oHandlerInitFunction:
      try:
        dRetVal = oHandlerInitFunction( self.oServiceConf  )
      except Exception, e:
        gLogger.exception()
        gLogger.fatal( "Can't call handler initialization function", str(e) )
        sys.exit( 1 )
      if not dRetVal[ 'OK' ]:
        gLogger.fatal( "Error in the initialization function", dRetVal[ 'Message' ] )
        sys.exit(1)
    else:
      self.uServiceInitializationData = None
          
    
  def __getServiceAddress( self ):
    iPort = self.oServiceConf.getPort()
    return ( "", iPort )
    
  def serve( self ):
    gLogger.info( "Handler up", "Serving from %s" % self.sServiceURL )
    while True:
      self.__handleRequest()
      #self.oThreadPool.processResults()
      
  def __handleRequest( self ):
    try:
      oClientTransport = self.oTransport.acceptConnection()
    except socket.error:
      return
    try:
      self.oDispatcher.lock()
      if self.__checkClientAddress( oClientTransport ):
        self.oThreadPool.generateJobAndQueueIt( self.processClient,
                                        args = ( oClientTransport, ),
                                        oExceptionCallback = self.processClientException )
        
    finally:
      self.oDispatcher.unlock()
    
  def __checkClientAddress( self, oClientTransport ):
    #TODO: Check that the IP is not banned
    return True
      
  def processClientException( self, oTJ, lExceptionInfo ):
    gLogger.exception( "Exception in thread", lException = lExceptionInfo )
    
  def processClient( self, oClientTransport ):
    stClientData = oClientTransport.receiveData( 1024 )
    gLogger.debug( "Received action from client", str( stClientData ) )
    dHandler = self.oDispatcher.getHandlerForService( stClientData[0][0] )
    if not dHandler:
      oClientTransport.sendData( S_ERROR( "Service '%s' does not exist" % stClientData[0][0] ) )
      oClientTransport.close()
      return
    try:
      oRH = dHandler[ "handlerClass" ]( stClientData[0], 
                      self.stServerAddress, 
                      oClientTransport,
                      dHandler[ "lockManager" ],
                      self.oServiceConf )
      oRH.initialize()
    except Exception, e:
      oClientTransport.sendData( S_ERROR( "Cannot process request; %s" % str(e) ) )
      raise
    oClientTransport.sendData( S_OK() )
    oRH.executeAction( stClientData[1] )
    oClientTransport.close()

  
if __name__=="__main__":
  oServer = Server( "Configuration")
  oServer.serve()
