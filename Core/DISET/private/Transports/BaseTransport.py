# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/DISET/private/Transports/BaseTransport.py,v 1.19 2008/10/16 13:28:38 acasajus Exp $
__RCSID__ = "$Id: BaseTransport.py,v 1.19 2008/10/16 13:28:38 acasajus Exp $"

from DIRAC.Core.Utilities.ReturnValues import S_ERROR, S_OK
from DIRAC.Core.Utilities import DEncode
from DIRAC.LoggingSystem.Client.Logger import gLogger
import select


class BaseTransport:

  bAllowReuseAddress = True
  iListenQueueSize = 5
  iReadTimeout = 600

  def __init__( self, stServerAddress, bServerMode = False, **kwargs ):
    self.bServerMode = bServerMode
    self.extraArgsDict = kwargs
    self.byteStream = ""
    self.packetSize = 1048576 #1MiB
    self.stServerAddress = stServerAddress
    self.peerCredentials = {}
    self.remoteAddress = False
    self.appData = ""

  def handshake(self):
    pass

  def setAppData( self, appData ):
    self.appData = appData

  def getAppData( self ):
    return self.appData

  def getConnectingCredentials( self ):
    return self.peerCredentials

  def setExtraCredentials( self, group ):
    self.peerCredentials[ 'extraCredentials' ] = group

  def serverMode( self ):
    return self.bServerMode

  def getTransportName( self ):
    return self.sTransportName

  def getRemoteAddress( self ):
    return self.remoteAddress

  def getLocalAddress( self ):
    return self.oSocket.getsockname()

  def getSocket( self ):
    return self.oSocket

  def _write( self, sBuffer ):
    self.oSocket.send( sBuffer )

  def _readReady( self ):
    if not self.iReadTimeout:
      return True
    inList, dummy, dummy = select.select( [ self.oSocket ], [], [], self.iReadTimeout )
    if self.oSocket in inList:
      return True
    return False

  #ADRI: Removed the readyCheck. It's controlled centrally
  def _read( self, bufSize = 4096, skipReadyCheck = True ):
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

  def sendData( self, uData ):
    sCodedData = DEncode.encode( uData )
    dataToSend = "%s:%s" % ( len( sCodedData ), sCodedData )
    for index in range( 0, len( dataToSend ), self.packetSize ):
      bytesToSend = len( dataToSend[ index : index + self.packetSize ] )
      packSentBytes = 0
      while packSentBytes < bytesToSend:
        try:
          sentBytes = self.oSocket.send( dataToSend[ index + packSentBytes : index + bytesToSend ] )
        except Exception, e:
          return S_ERROR( "Exception while sending data: %s" % e)
        if sentBytes == 0:
          return S_ERROR( "Connection closed by peer" )
        packSentBytes += sentBytes
    return S_OK()


  def receiveData( self, iMaxLength = 0 ):
    try:
      iSeparatorPosition = self.byteStream.find( ":" )
      while iSeparatorPosition == -1:
        retVal = self._read( 1024 )
        if not retVal[ 'OK' ]:
          return retVal
        self.byteStream += retVal[ 'Value' ]
        iSeparatorPosition = self.byteStream.find( ":" )
        if iMaxLength and len( self.byteStream ) > iMaxLength and iSeparatorPosition == -1 :
          raise RuntimeError( "Read limit exceeded (%s chars)" % iMaxLength )
      size = int( self.byteStream[ :iSeparatorPosition ] )
      self.byteStream = self.byteStream[ iSeparatorPosition+1: ]
      while len( self.byteStream ) < size:
        retVal = self._read( size - len( self.byteStream ), skipReadyCheck = True )
        if not retVal[ 'OK' ]:
          return retVal
        self.byteStream += retVal[ 'Value' ]
        if iMaxLength and len( self.byteStream ) > iMaxLength:
          raise RuntimeError( "Read limit exceeded (%s chars)" % iMaxLength )
      data = self.byteStream[ :size ]
      self.byteStream = self.byteStream[ size + 1: ]
      return DEncode.decode( data )[0]
    except Exception, e:
      gLogger.exception( "Network error while receiving data" )
      return S_ERROR( "Network error while receiving data: %s" % str( e ) )

