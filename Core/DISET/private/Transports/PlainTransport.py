# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/DISET/private/Transports/PlainTransport.py,v 1.3 2007/05/03 18:59:47 acasajus Exp $
__RCSID__ = "$Id: PlainTransport.py,v 1.3 2007/05/03 18:59:47 acasajus Exp $"

import socket
from DIRAC.Core.DISET.private.Transports.BaseTransport import BaseTransport
from DIRAC.LoggingSystem.Client.Logger import gLogger

class PlainTransport( BaseTransport ):

  def getUserInfo( self ):
    return {}

  def initAsClient( self ):
    self.oSocket = socket.socket( socket.AF_INET, socket.SOCK_STREAM )
    self.oSocket.connect( self.stServerAddress )
    return self.oSocket

  def initAsServer( self ):
    if not self.serverMode():
      raise RuntimeError( "Must be initialized as server mode" )
    self.oSocket = socket.socket( socket.AF_INET, socket.SOCK_STREAM )
    if self.bAllowReuseAddress:
      self.oSocket.setsockopt( socket.SOL_SOCKET, socket.SO_REUSEADDR, 1 )
    self.oSocket.bind( self.stServerAddress )
    self.oSocket.listen( self.iListenQueueSize )
    return self.oSocket

  def close( self ):
    gLogger.debug( "Closing socket" )
    self.oSocket.close()

  def setClientSocket( self, oSocket ):
    if self.serverMode():
      raise RuntimeError( "Mustbe initialized as client mode" )
    self.oSocket = oSocket

  def acceptConnection( self ):
    #HACK: Was = PlainTransport( self )
    oClientTransport = PlainTransport( self.stServerAddress )
    oClientSocket, stClientAddress = self.oSocket.accept()
    oClientTransport.setClientSocket( oClientSocket )
    return oClientTransport

