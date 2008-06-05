# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/DISET/private/Transports/PlainTransport.py,v 1.9 2008/06/05 10:20:17 acasajus Exp $
__RCSID__ = "$Id: PlainTransport.py,v 1.9 2008/06/05 10:20:17 acasajus Exp $"

import socket
from DIRAC.Core.DISET.private.Transports.BaseTransport import BaseTransport
from DIRAC.LoggingSystem.Client.Logger import gLogger

class PlainTransport( BaseTransport ):

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
    try:
      self.oSocket.shutdown( socket.SHUT_RDWR )
    except:
      pass
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

def checkSanity( *args, **kwargs ):
  return True

def delegate( delegationRequest, kwargs ):
  """
  Check delegate!
  """
  return S_OK()