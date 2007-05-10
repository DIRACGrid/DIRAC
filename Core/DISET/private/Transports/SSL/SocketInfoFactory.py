
import socket
from OpenSSL import SSL
from DIRAC.Core.DISET.private.Transports.SSL.SocketInfo import SocketInfo
from DIRAC.Core.DISET.private.Transports.SSL.SessionManager import SessionManager
from DIRAC.Core.DISET.private.Transports.SSL.FakeSocket import FakeSocket
from DIRAC.Core.DISET.private.Transports.SSL.ThreadSafeSSLObject import ThreadSafeSSLObject

class SocketInfoFactory:

  def __init__( self ):
    self.sessionManager = SessionManager()

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
    if self.sessionManager.isValidSession( sessionId ):
      sslSocket.set_session( self.sessionManager.getSession( sessionId ) )
      self.sessionManager.freeSession( sessionId )
    sslSocket.connect( hostAddress )
    socketInfo.doClientHandshake()
    self.sessionManager.setSession( sessionId, sslSocket.get_session() )

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