# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/DISET/private/GatewayDispatcher.py,v 1.10 2009/06/25 16:15:47 acasajus Exp $
__RCSID__ = "$Id: GatewayDispatcher.py,v 1.10 2009/06/25 16:15:47 acasajus Exp $"

import cStringIO
import DIRAC
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.ConfigurationSystem.Client.PathFinder import getServiceSection
from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData
from DIRAC.Core.DISET.private.FileHelper import FileHelper
from DIRAC.Core.Security.X509Chain import X509Chain

from DIRAC.Core.DISET.AuthManager import AuthManager
from DIRAC.Core.DISET.private.Dispatcher import Dispatcher
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.DISET.TransferClient import TransferClient
from DIRAC.Core.DISET.private.BaseClient import BaseClient

class GatewayDispatcher( Dispatcher ):

  gatewayServiceName = "Framework/Gateway"

  def __init__( self, serviceCfgList ):
    Dispatcher.__init__( self, serviceCfgList )
    self.__transferBytesLimit = 1024*1024*100 #100MiB

  def loadHandlers( self ):
    """
    No handler to load for the gateway
    """
    return S_OK()

  def initializeHandlers( self ):
    """
    No static initialization
    """
    return S_OK()

  def _lock( self, svcName ):
    pass

  def _unlock( self, svcName ):
    pass

  def _authorizeClientProposal( self, service, actionTuple, clientTransport ):
    """
    Authorize the action being proposed by the client
    """
    if actionTuple[0] == 'RPC':
      action = actionTuple[1]
    else:
      action = "%s/%s" % actionTuple
    credDict = clientTransport.getConnectingCredentials()
    retVal = self._authorizeAction( service, action, dict( credDict ) )
    if not retVal[ 'OK' ]:
      clientTransport.sendData( retVal )
      return False
    return True

  def _authorizeAction( self, serviceName, action, credDict ):
    try:
      authManager = AuthManager( "%s/Authorization" % getServiceSection( serviceName ) )
    except:
      return S_ERROR( "Service %s is unknown" % serviceName )
    gLogger.debug( "Trying credentials %s" % credDict )
    if not authManager.authQuery( action, credDict ):
      identity = "unknown"
      if 'username' in credDict:
        if 'group' in credDict:
          identity = "[%s:%s]" % ( credDict[ 'username' ], credDict[ 'group' ]  )
        else:
          identity = "[%s:unknown]" % credDict[ 'username' ]
      if 'DN' in credDict:
        identity += "(%s)" % credDict[ 'DN' ]
      gLogger.error( "Unauthorized query", "to %s:%s by %s" % ( serviceName, action, identity ) )
      return S_ERROR( "Unauthorized query to %s:%s" % ( serviceName, action ) )
    return S_OK()

  def _executeAction( self, proposalTuple, clientTransport ):
    """
    Execute an action
    """
    credDict = clientTransport.getConnectingCredentials()
    retVal = self.__checkDelegation( clientTransport )
    if not retVal[ 'OK' ]:
      return retVal
    delegatedChain = retVal[ 'Value' ]
    #Generate basic init args
    targetService = proposalTuple[0][0]
    clientInitArgs = {
                        BaseClient.KW_SETUP : proposalTuple[0][1],
                        BaseClient.KW_TIMEOUT : 600,
                        BaseClient.KW_IGNORE_GATEWAYS : True,
                        BaseClient.KW_USE_CERTIFICATES : False,
                        BaseClient.KW_PROXY_STRING : delegatedChain
                        }
    if BaseClient.KW_EXTRA_CREDENTIALS in credDict:
      clientInitArgs[ BaseClient.KW_EXTRA_CREDENTIALS ] = credDict[ BaseClient.KW_EXTRA_CREDENTIALS ]
    #OOkay! Lets do the magic!
    retVal = clientTransport.receiveData()
    if not retVal[ 'OK' ]:
      gLogger.error( "Error while receiving file description", retVal[ 'Message' ] )
      clientTransport.sendData(  S_ERROR( "Error while receiving file description: %s" % retVal[ 'Message' ] ) )
      return
    actionType = proposalTuple[1][0]
    actionMethod = proposalTuple[1][1]
    userDesc = self.__getUserDescription( credDict )
    #Filetransfer not here yet :P
    if actionType == "FileTransfer":
      gLogger.warn( "Received a file transfer action from %s" % userDesc )
      clientTransport.sendData( S_OK( "Accepted" ) )
      retVal =  self.__forwardFileTransferCall( targetService, clientInitArgs,
                                                actionMethod, retVal[ 'Value' ], clientTransport )
    elif actionType == "RPC":
      gLogger.info( "Forwarding %s/%s action to %s for %s" % ( actionType, actionMethod, targetService, userDesc ) )
      retVal = self.__forwardRPCCall( targetService, clientInitArgs, actionMethod, retVal[ 'Value' ] )
    else:
      gLogger.warn( "Received an unknown %s action from %s" % ( actionType, userDesc ) )
      retVal =  S_ERROR( "Unknown type of action (%s)" % actionType )
    clientTransport.sendData( retVal )

  def __checkDelegation( self, clientTransport ):
    """
    Check the delegation
    """
    #Ask for delegation
    credDict = clientTransport.getConnectingCredentials()
    #If it's not secure don't ask for delegation
    if 'x509Chain' not in credDict:
      return S_OK()
    peerChain = credDict[ 'x509Chain' ]
    retVal = peerChain.getCertInChain()[ 'Value' ].generateProxyRequest()
    if not retVal[ 'OK' ]:
      return retVal
    delegationRequest = retVal[ 'Value' ]
    retVal = delegationRequest.dumpRequest()
    if not retVal[ 'OK' ]:
      retVal = S_ERROR( "Server Error: Can't generate delegation request" )
      clientTransport.sendData( retVal )
      return retVal
    gLogger.info( "Sending delegation request for %s" % delegationRequest.getSubjectDN()[ 'Value' ] )
    clientTransport.sendData( S_OK( { 'delegate' : retVal[ 'Value' ] } ) )
    delegatedCertChain = clientTransport.receiveData()
    delegatedChain = X509Chain( keyObj = delegationRequest.getPKey() )
    retVal = delegatedChain.loadChainFromString( delegatedCertChain )
    if not retVal[ 'OK' ]:
      retVal = S_ERROR( "Error in receiving delegated proxy: %s" % retVal[ 'Message' ] )
      clientTransport.sendData( retVal )
      return retVal
    clientTransport.sendData( S_OK() )
    return delegatedChain.dumpAllToString()

  def __getUserDescription( self, credDict ):
    if 'DN' not in credDict:
      DN = "anonymous"
    else:
      DN = credDict[ 'DN' ]
    if 'group' not in credDict:
      group = "unknownGroup"
    else:
      group = credDict[ 'group' ]
    return "%s@%s" % ( DN, group )

  def __forwardRPCCall( self, targetService, clientInitArgs, method, params ):
    if targetService == "Configuration/Server":
      if method == "getCompressedDataIfNewer":
        #Relay CS data directly
        serviceVersion = gConfigurationData.getVersion()
        retDict = { 'newestVersion' : serviceVersion }
        clientVersion = params[0]
        if clientVersion < serviceVersion:
          retDict[ 'data' ] = gConfigurationData.getCompressedData()
        return S_OK( retDict )
    #Default
    rpcClient = RPCClient( targetService, **clientInitArgs )
    methodObj = getattr( rpcClient, method )
    return methodObj( *params )

  def __forwardFileTransferCall( self, targetService, clientInitArgs, method,
                                 params, clientTransport ):
    transferRelay = TransferRelay( targetService, **clientInitArgs )
    transferRelay.setTransferLimit( self.__transferBytesLimit )
    cliFH = FileHelper( clientTransport )
    #Check file size
    if method.find( "ToClient" ) > -1:
      cliFH.setDirection( "send" )
    elif method.find( "FromClient" ) > -1:
      cliFH.setDirection( "receive" )
      if not self.__ftCheckMaxTransferSize( params[2] ):
        cliFH.markAsTransferred()
        return S_ERROR( "Transfer size is too big" )
    #Forward queries
    try:
      relayMethodObject = getattr( transferRelay, 'forward%s' % method )
    except:
      return S_ERROR( "Cannot forward unknown method %s" % method )
    result = relayMethodObject( cliFH, params )
    return result

  def __ftCheckMaxTransferSize( self, requestedTransferSize ):
    if not self.__transferBytesLimit:
      return True
    if not requestedTransferSize:
      return True
    if requestedTransferSize <= self.__transferBytesLimit:
      return True
    return False

  def __ftFromClient( self, sIO, fileHelper, fileId, token ):
    gLogger.info( "[FromClient] About to get data from client" )
    result = fileHelper.networkToDataSink( sIO, self.__transferBytesLimit )
    if not result[ 'OK' ]:
      return result
    gLogger.info( "[FromClient] Got %s bytes from client" % fileHelper.getTransferedBytes() )
    result = transferClient.sendFile( sIO, fileId, token )
    gLogger.info( "[FromClient] Sent data to service %s (%s)" % ( targetService, result ) )
    return result


class TransferRelay( TransferClient ):

  def setTransferLimit( self, trLimit ):
    self.__transferBytesLimit = trLimit
    self.__currentMethod = ""

  def infoMsg( self, msg, dynMsg = "" ):
    gLogger.info( "[%s] %s" % ( self.__currentMethod, msg ), dynMsg )

  def errMsg( self, msg, dynMsg = "" ):
    gLogger.error( "[%s] %s" % ( self.__currentMethod, msg ), dynMsg )

  def getDataFromClient( self, clientFileHelper ):
    sIO = cStringIO.StringIO()
    self.infoMsg( "About to get data from client" )
    result = clientFileHelper.networkToDataSink( sIO, self.__transferBytesLimit )
    if not result[ 'OK' ]:
      sIO.close()
      self.errMsg( "Could not get data from client", result[ 'Message' ] )
      return result
    data = sIO.getvalue()
    sIO.close()
    self.infoMsg( "Got %s bytes from client" % len( data ) )
    return S_OK( data )

  def sendDataToClient( self, clientFileHelper, dataToSend ):
    self.infoMsg( "About to get send data to client" )
    result = clientFileHelper.BufferToNetwork( dataToSend )
    if not result[ 'OK' ]:
      self.errMsg( "Could not send data to client", result[ 'Message' ] )
      return result
    self.infoMsg( "Sent %s bytes from client" % len( dataToSend ) )
    return S_OK()

  def sendDataToService( self, srvMethod, params, data ):
    self.infoMsg( "Sending header request to %s" % self.serviceName, str( params ) )
    result = self._sendTransferHeader( srvMethod, params )
    if not result[ 'OK' ]:
      self.errMsg( "Could not send header", result[ 'Message' ] )
      return result
    self.infoMsg( "Starting to send data to service" )
    srvTransport = result[ 'Value' ]
    srvFileHelper = FileHelper( srvTransport )
    srvFileHelper.setDirection( "send" )
    result = srvFileHelper.BufferToNetwork( data )
    if not result[ 'OK' ]:
      self.errMsg( "Could send data to server", result[ 'Message' ] )
      srvTransport.close()
      return result
    self.infoMsg( "Data sent to service (%s bytes)" % len( data ) )
    retVal = srvTransport.receiveData()
    srvTransport.close()
    return retVal

  def getDataFromService( self, srvMethod, params ):
    self.infoMsg( "Sending header request to %s" % self.serviceName, str( params ) )
    result = self._sendTransferHeader( srvMethod, params )
    if not result[ 'OK' ]:
      self.errMsg( "Could not send header", result[ 'Message' ] )
      return result
    self.infoMsg( "Starting to receive data from service" )
    srvTransport = result[ 'Value' ]
    srvFileHelper = FileHelper( srvTransport )
    srvFileHelper.setDirection( "receive" )
    sIO = cStringIO.StringIO()
    result = srvFileHelper.networkToDataSink( sIO, self.__transferBytesLimit )
    if not result[ 'OK' ]:
      self.errMsg( "Could receive data from server", result[ 'Message' ] )
      srvTransport.close()
      sIO.close()
      return result
    dataReceived = sIO.getvalue()
    sIO.close()
    self.infoMsg( "Received %s bytes from service" % len( dataReceived ) )
    retVal = srvTransport.receiveData()
    srvTransport.close()
    if not retVal[ 'OK' ]:
      return retVal
    return S_OK( { 'data' : dataReceived, 'srvResponse' : retVal } )

  def forwardFromClient( self, clientFileHelper, params ):
    print params
    fileId, token = params[:2]
    self.__currentMethod = "FromClient"
    result = self.getDataFromClient( clientFileHelper )
    if not result[ 'OK' ]:
      return result
    dataReceived = result[ 'Value' ]
    receivedBytes = clientFileHelper.getTransferedBytes()
    return self.sendDataToService( "FromClient", ( fileId, token, receivedBytes ), dataReceived )

  def forwardBulkFromClient( self, clientFileHelper, params ):
    fileId, token = params[:2]
    self.__currentMethod = "BulkFromClient"
    result = self.getDataFromClient( clientFileHelper )
    if not result[ 'OK' ]:
      return result
    dataReceived = result[ 'Value' ]
    receivedBytes = clientFileHelper.getTransferedBytes()
    return self.sendDataToService( "BulkFromClient", ( fileId, token, receivedBytes ), dataReceived )

  def forwardToClient( self, clientFileHelper, params ):
    print params
    fileId, token = params[:2]
    self.__currentMethod = "ToClient"
    result = self.getDataFromService( "ToClient", ( fileId, token ) )
    if not result[ 'OK' ]:
      return result
    dataReceived = result[ 'Value' ][ 'data' ]
    srvResponse = result[ 'Value' ][ 'srvResponse' ]
    result = self.sendDataToClient( clientFileHelper, dataReceived )
    if not result[ 'OK' ]:
      return result
    return srvResponse

  def forwardBulkToClient( self, clientFileHelper, params ):
    print params
    fileId, token = params[:2]
    self.__currentMethod = "BulkToClient"
    result = self.getDataFromService( "BulkToClient", ( fileId, token ) )
    if not result[ 'OK' ]:
      return result
    dataReceived = result[ 'Value' ][ 'data' ]
    srvResponse = result[ 'Value' ][ 'srvResponse' ]
    result = self.sendDataToClient( clientFileHelper, dataReceived )
    if not result[ 'OK' ]:
      return result
    return srvResponse