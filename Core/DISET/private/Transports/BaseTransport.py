# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/DISET/private/Transports/BaseTransport.py,v 1.10 2007/06/13 19:28:59 acasajus Exp $
__RCSID__ = "$Id: BaseTransport.py,v 1.10 2007/06/13 19:28:59 acasajus Exp $"

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
    self.packetSize = 8192
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

  def _write( self, sBuffer ):
    self.oSocket.send( sBuffer )

  def _read( self ):
    try:
      return self.oSocket.recv( self.packetSize )
    except socket.error:
      return ""

  def sendData( self, uData ):
    sCodedData = encode( uData )
    dataToSend = "%s:%s" % ( len( sCodedData ), sCodedData )
    for index in range( 0, len( dataToSend ), self.packetSize ):
      self.oSocket.send( dataToSend[ index : index + self.packetSize ] )

  def receiveData( self, iMaxLength = 0 ):
    try:
      iSeparatorPosition = self.byteStream.find( ":" )
      while iSeparatorPosition == -1:
        sReadData = self._read()
        if sReadData == "":
          break
        self.byteStream += sReadData
        iSeparatorPosition = self.byteStream.find( ":" )
        if iMaxLength and len( self.byteStream ) > iMaxLength and iSeparatorPosition == -1 :
          raise RuntimeError( "Read limit exceeded (%s chars)" % iMaxLength )
      size = int( self.byteStream[ :iSeparatorPosition ] )
      self.byteStream = self.byteStream[ iSeparatorPosition+1: ]
      while len( self.byteStream ) < size:
        self.byteStream += self._read()
        if iMaxLength and len( self.byteStream ) > iMaxLength:
          raise RuntimeError( "Read limit exceeded (%s chars)" % iMaxLength )
      data = self.byteStream[ :size ]
      self.byteStream = self.byteStream[ size + 1: ]
      return decode( data )[0]
    except Exception, e:
      gLogger.exception( "Network error while receiving data" )
      return S_ERROR( "Network error while receiving data: %s" % str( e ) )

