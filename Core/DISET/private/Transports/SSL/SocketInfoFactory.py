# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/DISET/private/Transports/SSL/SocketInfoFactory.py,v 1.4 2007/05/16 10:06:58 acasajus Exp $
__RCSID__ = "$Id: SocketInfoFactory.py,v 1.4 2007/05/16 10:06:58 acasajus Exp $"

import socket
from OpenSSL import SSL
from DIRAC.Core.DISET.private.Transports.SSL.SocketInfo import SocketInfo
from DIRAC.Core.DISET.private.Transports.SSL.ContextManager import gContextManager
from DIRAC.Core.DISET.private.Transports.SSL.FakeSocket import FakeSocket
from DIRAC.Core.DISET.private.Transports.SSL.ThreadSafeSSLObject import ThreadSafeSSLObject

class SocketInfoFactory:

  def generateClientInfo( self, destinationHostname, kwargs ):
    infoDict = { 'clientMode' : True, 'hostname' : destinationHostname }
    for key in kwargs.keys():
      infoDict[ key ] = kwargs[ key ]
    return SocketInfo( infoDict )

  def generateServerInfo( self, kwargs ):
    infoDict = { 'clientMode' : False }
    for key in kwargs.keys():
      infoDict[ key ] = kwargs[ key ]
    return SocketInfo( infoDict )

  def getSocket( self, hostAddress, **kwargs ):
    osSocket = socket.socket( socket.AF_INET, socket.SOCK_STREAM )
    socketInfo = self.generateClientInfo( hostAddress[0], kwargs )
    sslSocket = SSL.Connection( socketInfo.getSSLContext(), osSocket )
    sslSocket = ThreadSafeSSLObject( sslSocket )
    sslSocket = FakeSocket( sslSocket )
    socketInfo.setSSLSocket( sslSocket )
    sessionId = ":".join( socketInfo.getLocalCredentialsLocation() )
    if gContextManager.isValidSession( sessionId ):
      sslSocket.set_session( self.sessionManager.getSession( sessionId ) )
      gContextManager.freeSession( sessionId )
    gContextManager.setSession( sessionId, sslSocket.get_session() )
    sslSocket.connect( hostAddress )
    socketInfo.doClientHandshake()

    return socketInfo

  def getListeningSocket( self, hostAddress, listeningQueueSize = 5, reuseAddress = True, **kwargs ):
    osSocket = socket.socket( socket.AF_INET, socket.SOCK_STREAM )
    if reuseAddress:
      osSocket.setsockopt( socket.SOL_SOCKET, socket.SO_REUSEADDR, 1 )
    socketInfo = self.generateServerInfo( kwargs )
    sslSocket = SSL.Connection( socketInfo.getSSLContext(), osSocket )
    sslSocket.bind( hostAddress )
    sslSocket.listen( listeningQueueSize )
    socketInfo.setSSLSocket( sslSocket )
    return socketInfo

gSocketInfoFactory = SocketInfoFactory()