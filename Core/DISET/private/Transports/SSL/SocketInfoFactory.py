
import socket
from OpenSSL import SSL
from DIRAC.Core.DISET.private.Transports.SSL.SocketInfo import SocketInfo
from DIRAC.Core.DISET.private.Transports.SSL.SessionManager import SessionManager
from DIRAC.Core.DISET.private.Transports.SSL.FakeSocket import FakeSocket
from DIRAC.Core.DISET.private.Transports.SSL.ThreadSafeSSLObject import ThreadSafeSSLObject

class SocketInfoFactory:

  def __init__( self ):
    self.sessionManager = SessionManager()

  def generateClientInfo( self, destinationHostname ):
    infoDict = { 'clientMode' : True, 'hostname' : destinationHostname }
    return SocketInfo( infoDict )

  def generateServerInfo( self ):
    infoDict = { 'clientMode' : False }
    return SocketInfo( infoDict )

  def getSocket( self, hostAddress ):
    osSocket = socket.socket( socket.AF_INET, socket.SOCK_STREAM )
    socketInfo = self.generateClientInfo( hostAddress[0] )
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

  def getListeningSocket( self, hostAddress, listeningQueueSize = 5, reuseAddress = True ):
    osSocket = socket.socket( socket.AF_INET, socket.SOCK_STREAM )
    if reuseAddress:
      osSocket.setsockopt( socket.SOL_SOCKET, socket.SO_REUSEADDR, 1 )
    socketInfo = self.generateServerInfo()
    sslSocket = SSL.Connection( socketInfo.getSSLContext(), osSocket )
    sslSocket.bind( hostAddress )
    sslSocket.listen( listeningQueueSize )
    socketInfo.setSSLSocket( sslSocket )
    return socketInfo

gSocketInfoFactory = SocketInfoFactory()