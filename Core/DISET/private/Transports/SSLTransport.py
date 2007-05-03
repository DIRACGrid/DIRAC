# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/DISET/private/Transports/SSLTransport.py,v 1.3 2007/05/03 18:59:47 acasajus Exp $
__RCSID__ = "$Id: SSLTransport.py,v 1.3 2007/05/03 18:59:47 acasajus Exp $"

import OpenSSL
from DIRAC.Core.DISET.private.Transports.BaseTransport import BaseTransport
from DIRAC.LoggingSystem.Client.Logger import gLogger
from DIRAC.Core.DISET.private.Transports.SSL.SocketInfoFactory import gSocketInfoFactory

class SSLTransport( BaseTransport ):

  def getUserInfo( self ):
    return self.peerCredentials

  def initAsClient( self ):
    self.oSocketInfo = gSocketInfoFactory.getSocket( self.stServerAddress )
    self.oSocket = self.oSocketInfo.getSSLSocket()

  def initAsServer( self ):
    if not self.serverMode():
      raise RuntimeError( "Must be initialized as server mode" )
    self.oSocketInfo = gSocketInfoFactory.getListeningSocket( self.stServerAddress,
                                                      self.iListenQueueSize,
                                                      self.bAllowReuseAddress )
    self.oSocket = self.oSocketInfo.getSSLSocket()

  def close( self ):
    gLogger.debug( "Closing socket" )
    self.oSocket.close()

  def handshake( self ):
    self.peerCredentials = self.oSocketInfo.doServerHandshake()

  def setClientSocket( self, oSocket ):
    if self.serverMode():
      raise RuntimeError( "Must be initialized as client mode" )
    self.oSocketInfo.setSSLSocket( oSocket )
    self.oSocket = oSocket

  def acceptConnection( self ):
    oClientTransport = SSLTransport( self.stServerAddress )
    oClientSocket, stClientAddress = self.oSocket.accept()
    oClientTransport.oSocketInfo = self.oSocketInfo.clone()
    oClientTransport.setClientSocket( oClientSocket )
    return oClientTransport

