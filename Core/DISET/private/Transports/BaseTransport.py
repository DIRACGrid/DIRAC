# $HeadURL$
__RCSID__ = "$Id$"

import time
import select
import cStringIO
try:
  from hashlib import md5
except:
  from md5 import md5

from DIRAC.Core.Utilities.ReturnValues import S_ERROR, S_OK
from DIRAC.Core.Utilities import DEncode
from DIRAC.FrameworkSystem.Client.Logger import gLogger

class BaseTransport:

  bAllowReuseAddress = True
  iListenQueueSize = 5
  iReadTimeout = 600
  keepAliveMagic = "dka"

  def __init__( self, stServerAddress, bServerMode = False, **kwargs ):
    self.bServerMode = bServerMode
    self.extraArgsDict = kwargs
    self.byteStream = ""
    self.packetSize = 1048576 #1MiB
    self.stServerAddress = stServerAddress
    self.peerCredentials = {}
    self.remoteAddress = False
    self.appData = ""
    self.startedKeepAlives = set()
    self.keepAliveId = md5( str( stServerAddress ) + str( bServerMode ) ).hexdigest()
    self.receivedMessages = []
    self.sentKeepAlives = 0
    self.waitingForKeepAlivePong = False
    self.__keepAliveLapse = 0
    self.oSocket = None
    if 'keepAliveLapse' in kwargs:
      try:
        self.__keepAliveLapse = max( 150, int( kwargs[ 'keepAliveLapse' ] ) )
      except:
        pass
    self.__lastActionTimestamp = time.time()
    self.__lastServerRenewTimestamp = self.__lastActionTimestamp

  def __updateLastActionTimestamp( self ):
    self.__lastActionTimestamp = time.time()

  def getLastActionTimestamp( self ):
    return self.__lastActionTimestamp

  def getKeepAliveLapse( self ):
    return self.__keepAliveLapse

  def handshake( self ):
    return S_OK()

  def close( self ):
    self.oSocket.close()

  def setAppData( self, appData ):
    self.appData = appData

  def getAppData( self ):
    return self.appData

  def renewServerContext( self ):
    self.__lastServerRenewTimestamp = time.time()
    return S_OK()

  def latestServerRenewTime( self ):
    return self.__lastServerRenewTimestamp

  def getConnectingCredentials( self ):
    return self.peerCredentials

  def setExtraCredentials( self, group ):
    self.peerCredentials[ 'extraCredentials' ] = group

  def serverMode( self ):
    return self.bServerMode

  def getRemoteAddress( self ):
    return self.remoteAddress

  def getLocalAddress( self ):
    return self.oSocket.getsockname()

  def getSocket( self ):
    return self.oSocket

  def _readReady( self ):
    if not self.iReadTimeout:
      return True
    inList, dummy, dummy = select.select( [ self.oSocket ], [], [], self.iReadTimeout )
    if self.oSocket in inList:
      return True
    return False

  def _read( self, bufSize = 4096, skipReadyCheck = False ):
    try:
      if skipReadyCheck or self._readReady():
        data = self.oSocket.recv( bufSize )
        if not data:
          return S_ERROR( "Connection closed by peer" )
        else:
          return S_OK( data )
      else:
        return S_ERROR( "Connection seems stalled. Closing..." )
    except Exception, e:
      return S_ERROR( "Exception while reading from peer: %s" % str( e ) )

  def _write( self, buffer ):
    return S_OK( self.oSocket.send( buffer ) )

  def sendData( self, uData, prefix = False ):
    self.__updateLastActionTimestamp()
    sCodedData = DEncode.encode( uData )
    if prefix:
      dataToSend = "%s%s:%s" % ( prefix, len( sCodedData ), sCodedData )
    else:
      dataToSend = "%s:%s" % ( len( sCodedData ), sCodedData )
    for index in range( 0, len( dataToSend ), self.packetSize ):
      bytesToSend = min( self.packetSize, len( dataToSend ) - index )
      packSentBytes = 0
      while packSentBytes < bytesToSend:
        try:
          result = self._write( dataToSend[ index + packSentBytes : index + bytesToSend ] )
          if not result[ 'OK' ]:
            return result
          sentBytes = result[ 'Value' ]
        except Exception, e:
          return S_ERROR( "Exception while sending data: %s" % e )
        if sentBytes == 0:
          return S_ERROR( "Connection closed by peer" )
        packSentBytes += sentBytes
    return S_OK()


  def receiveData( self, maxBufferSize = 0, blockAfterKeepAlive = True, idleReceive = False ):
    self.__updateLastActionTimestamp()
    if self.receivedMessages:
      return self.receivedMessages.pop( 0 )
    #Buffer size can't be less than 0
    maxBufferSize = max( maxBufferSize, 0 )
    try:
      #Look either for message length of keep alive magic string
      iSeparatorPosition = self.byteStream.find( ":", 0, 10 )
      keepAliveMagicLen = len( BaseTransport.keepAliveMagic )
      isKeepAlive = self.byteStream.find( BaseTransport.keepAliveMagic, 0, keepAliveMagicLen ) == 0
      #While not found the message length or the ka, keep receiving
      while iSeparatorPosition == -1 and not isKeepAlive:
        retVal = self._read( 16384 )
        #If error return
        if not retVal[ 'OK' ]:
          return retVal
        #If closed return error
        if not retVal[ 'Value' ]:
          return S_ERROR( "Peer closed connection" )
        #New data!
        self.byteStream += retVal[ 'Value' ]
        #Look again for either message length of ka magic string
        iSeparatorPosition = self.byteStream.find( ":", 0, 10 )
        isKeepAlive = self.byteStream.find( BaseTransport.keepAliveMagic, 0, keepAliveMagicLen ) == 0
        #Over the limit?
        if maxBufferSize and len( self.byteStream ) > maxBufferSize and iSeparatorPosition == -1 :
          return S_ERROR( "Read limit exceeded (%s chars)" % maxBufferSize )
      #Keep alive magic!
      if isKeepAlive:
        gLogger.debug( "Received keep alive header" )
        #Remove the ka magic from the buffer and process the keep alive
        self.byteStream = self.byteStream[ keepAliveMagicLen: ]
        return self.__processKeepAlive( maxBufferSize, blockAfterKeepAlive )
      #From here it must be a real message!
      #Process the size and remove the msg length from the bytestream
      pkgSize = int( self.byteStream[ :iSeparatorPosition ] )
      pkgData = self.byteStream[ iSeparatorPosition + 1: ]
      readSize = len( pkgData )
      if readSize >= pkgSize:
        #If we already have all the data we need
        data = pkgData[ :pkgSize ]
        self.byteStream = pkgData[ pkgSize: ]
      else:
        #If we still need to read stuff
        pkgMem = cStringIO.StringIO()
        pkgMem.write( pkgData )
        #Receive while there's still data to be received
        while readSize < pkgSize:
          retVal = self._read( pkgSize - readSize, skipReadyCheck = True )
          if not retVal[ 'OK' ]:
            return retVal
          if not retVal[ 'Value' ]:
            return S_ERROR( "Peer closed connection" )
          rcvData = retVal[ 'Value' ]
          readSize += len( rcvData )
          pkgMem.write( rcvData )
          if maxBufferSize and readSize > maxBufferSize:
            return S_ERROR( "Read limit exceeded (%s chars)" % maxBufferSize )
        #Data is here! take it out from the bytestream, dencode and return
        if readSize == pkgSize:
          data = pkgMem.getvalue()
          self.byteStream = ""
        else: #readSize > pkgSize:
          pkgMem.seek( 0, 0 )
          data = pkgMem.read( pkgSize )
          self.byteStream = pkgMem.read()
      try:
        data = DEncode.decode( data )[0]
      except Exception, e:
        return S_ERROR( "Could not decode received data: %s" % str( e ) )
      if idleReceive:
        self.receivedMessages.append( data )
        return S_OK()
      return data
    except Exception, e:
      gLogger.exception( "Network error while receiving data" )
      return S_ERROR( "Network error while receiving data: %s" % str( e ) )

  def __processKeepAlive( self, maxBufferSize, blockAfterKeepAlive = True ):
    gLogger.debug( "Received Keep Alive" )
    #Next message down the stream will be the ka data
    result = self.receiveData( maxBufferSize, blockAfterKeepAlive = False )
    if not result[ 'OK' ]:
      gLogger.debug( "Error while receiving keep alive: %s" % result[ 'Message' ] )
      return result
    #Is it a valid ka?
    kaData = result[ 'Value' ]
    for reqField in ( 'id', 'kaping' ):
      if reqField not in kaData:
        errMsg = "Invalid keep alive, missing %s" % reqField
        gLogger.debug( errMsg )
        return S_ERROR( errMsg )
    gLogger.debug( "Received keep alive id %s" % kaData )
    #Need to check if it's one of the keep alives we sent or one started from the other side
    if kaData[ 'kaping' ]:
      #This is a keep alive PING. Let's send the PONG
      self.sendKeepAlive( responseId = kaData[ 'id' ] )
    else:
      #If it's a pong then we flag that we don't need to wait for a pong
      self.waitingForKeepAlivePong = False
    #No blockAfterKeepAlive means return without further read
    if not blockAfterKeepAlive:
      result = S_OK()
      result[ 'keepAlive' ] = True
      return result
    #Let's listen for the next message downstream
    return self.receiveData( maxBufferSize, blockAfterKeepAlive )

  def sendKeepAlive( self, responseId = None, now = False ):
    #If not responseId or not keepAliveLapse or not enough time has passed don't send keep alive
    if not responseId:
      if not self.__keepAliveLapse:
        return S_OK()
      if not now:
        now = time.time()
      if now - self.__lastActionTimestamp < self.__keepAliveLapse:
        return S_OK()

    self.__updateLastActionTimestamp()
    if responseId:
      self.waitingForKeepAlivePong = False
      kaData = S_OK( { 'id' : responseId, 'kaping' : False } )
    else:
      if self.waitingForKeepAlivePong:
        return S_OK()
      id = self.keepAliveId + str( self.sentKeepAlives )
      self.sentKeepAlives += 1
      kaData = S_OK( { 'id' : id, 'kaping' : True } )
      self.waitingForKeepAlivePong = True
    return self.sendData( kaData , prefix = BaseTransport.keepAliveMagic )

  def getFormattedCredentials( self ):
    peerCreds = self.getConnectingCredentials()
    address = self.getRemoteAddress()
    if peerCreds.has_key( 'username' ):
      peerId = "[%s:%s]" % ( peerCreds[ 'group' ], peerCreds[ 'username' ] )
    else:
      peerId = ""
    return "(%s:%s)%s" % ( address[0],
                           address[1],
                           peerId )
