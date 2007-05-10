# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/DISET/private/Transports/BaseTransport.py,v 1.4 2007/05/10 18:44:57 acasajus Exp $
__RCSID__ = "$Id: BaseTransport.py,v 1.4 2007/05/10 18:44:57 acasajus Exp $"

from DIRAC.Core.DISET.private.Transports.DEncode import encode, decode
from DIRAC.LoggingSystem.Client.Logger import gLogger

class BaseTransport:

  bAllowReuseAddress = True
  iListenQueueSize = 5

  def __init__( self, stServerAddress, bServerMode = False, **kwargs ):
    self.bServerMode = bServerMode
    self.extraArgsDict = kwargs
    self.sReadBuffer = ""
    self.stServerAddress = stServerAddress
    self.peerCredentials = {}

  def getConnectingCredentials( self ):
    return self.peerCredentials

  def setDisetGroup( self, group ):
    self.peerCredentials[ 'disetGroup' ] = group

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
      return self.oSocket.recv( 8192 )
    except socket.error:
      return ""

  def sendData( self, uData ):
    sCodedData = encode( uData )
    self._write( "%s:%s" % ( len( sCodedData ), sCodedData ) )

  def receiveData( self, iMaxLength = 0 ):
    iSeparatorPosition = self.sReadBuffer.find( ":" )
    while iSeparatorPosition == -1:
      sReadData = self._read()
      if sReadData == "":
        break
      self.sReadBuffer += sReadData
      iSeparatorPosition = self.sReadBuffer.find( ":" )
      if iMaxLength and len( self.sReadBuffer ) > iMaxLength and iSeparatorPosition == -1 :
        raise RuntimeError( "Read limit exceeded (%s chars)" % iMaxLength )
    iDataLength = int( self.sReadBuffer[ :iSeparatorPosition ] )
    sData = self.sReadBuffer[ iSeparatorPosition+1 : iSeparatorPosition+1+iDataLength ]
    self.sReadBuffer = self.sReadBuffer[ iSeparatorPosition+1+iDataLength: ]
    return decode( sData )[0]

  def handshake( self ):
    #Nothing to do if we are not SSL
    pass

