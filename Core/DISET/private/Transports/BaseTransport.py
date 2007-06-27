# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/DISET/private/Transports/BaseTransport.py,v 1.13 2007/06/27 18:19:25 acasajus Exp $
__RCSID__ = "$Id: BaseTransport.py,v 1.13 2007/06/27 18:19:25 acasajus Exp $"

from DIRAC.Core.Utilities.ReturnValues import S_ERROR
from DIRAC.Core.DISET.private.Transports.DEncode import encode, decode
from DIRAC.LoggingSystem.Client.Logger import gLogger
import socket

class BaseTransport:

  bAllowReuseAddress = True
  iListenQueueSize = 5

  def __init__( self, stServerAddress, bServerMode = False, **kwargs ):
    self.bServerMode = bServerMode
    self.extraArgsDict = kwargs
    self.byteStream = ""
    self.packetSize = 1048576 #1MiB
    self.stServerAddress = stServerAddress
    self.peerCredentials = {}

  def handshake(self):
    pass

  def getConnectingCredentials( self ):
    return self.peerCredentials

  def setDisetGroup( self, group ):
    self.peerCredentials[ 'group' ] = group

  def serverMode( self ):
    return self.bServerMode

  def getTransportName( self ):
    return self.sTransportName

  def getRemoteAddress( self ):
    return self.oSocket.getpeername()

  def getLocalAddress( self ):
    return self.oSocket.getsockname()

  def getSocket( self ):
    return self.oSocket

  def _write( self, sBuffer ):
    self.oSocket.send( sBuffer )

  def _read( self, bufSize = 4096 ):
    try:
      return self.oSocket.recv( bufSize )
    except socket.error:
      return ""

  def sendData( self, uData ):
    sCodedData = encode( uData )
    dataToSend = "%s:%s" % ( len( sCodedData ), sCodedData )
    for index in range( 0, len( dataToSend ), self.packetSize ):
      bytesToSend = len( dataToSend[ index : index + self.packetSize ] )
      packSentBytes = 0
      while packSentBytes < bytesToSend:
        sentBytes = self.oSocket.send( dataToSend[ index + packSentBytes : index + bytesToSend ] )
        if sentBytes == 0:
          raise Exception( "Connection closed by peer" )
        packSentBytes += sentBytes


  def receiveData( self, iMaxLength = 0 ):
    try:
      iSeparatorPosition = self.byteStream.find( ":" )
      while iSeparatorPosition == -1:
        sReadData = self._read( 1024 )
        if sReadData == "":
          return S_ERROR( "Connection closed by peer" )
        self.byteStream += sReadData
        iSeparatorPosition = self.byteStream.find( ":" )
        if iMaxLength and len( self.byteStream ) > iMaxLength and iSeparatorPosition == -1 :
          raise RuntimeError( "Read limit exceeded (%s chars)" % iMaxLength )
      size = int( self.byteStream[ :iSeparatorPosition ] )
      self.byteStream = self.byteStream[ iSeparatorPosition+1: ]
      while len( self.byteStream ) < size:
        self.byteStream += self._read( size - len( self.byteStream ) )
        if iMaxLength and len( self.byteStream ) > iMaxLength:
          raise RuntimeError( "Read limit exceeded (%s chars)" % iMaxLength )
      data = self.byteStream[ :size ]
      self.byteStream = self.byteStream[ size + 1: ]
      return decode( data )[0]
    except Exception, e:
      gLogger.exception( "Network error while receiving data" )
      return S_ERROR( "Network error while receiving data: %s" % str( e ) )

