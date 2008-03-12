# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/DISET/private/Transports/SSL/SocketInfoFactory.py,v 1.9 2008/03/12 20:18:25 acasajus Exp $
__RCSID__ = "$Id: SocketInfoFactory.py,v 1.9 2008/03/12 20:18:25 acasajus Exp $"

import socket
import GSI
from DIRAC.Core.DISET.private.Transports.SSL.SocketInfo import SocketInfo
from DIRAC.Core.DISET.private.Transports.SSL.SessionManager import gSessionManager
from DIRAC.Core.DISET.private.Transports.SSL.FakeSocket import FakeSocket
from DIRAC.Core.DISET.private.Transports.SSL.ThreadSafeSSLObject import ThreadSafeSSLObject

requiredGSIVersion = "0.2"
if GSI.version.__version__ < requiredGSIVersion:
  raise Exception( "pyGSI is not the latest version (installed %s required %s)" % ( GSI.version.__version__, requiredGSIVersion ) )

GSI.SSL.set_thread_safe()

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
    sslSocket = GSI.SSL.Connection( socketInfo.getSSLContext(), osSocket )
    #sslSocket = ThreadSafeSSLObject( sslSocket )
    #sslSocket = FakeSocket( sslSocket )
    sessionId = str( hash( str( hostAddress ) + ":".join( socketInfo.getLocalCredentialsLocation() )  ) )
    socketInfo.sslContext.set_session_id( str( hash( sessionId ) ) )
    socketInfo.setSSLSocket( sslSocket )
    if gSessionManager.isValid( sessionId ):
      sslSocket.set_session( gSessionManager.get( sessionId ) )
    sslSocket.connect( hostAddress )
    socketInfo.doClientHandshake()
    gSessionManager.set( sessionId, sslSocket.get_session() )

    return socketInfo

  def getListeningSocket( self, hostAddress, listeningQueueSize = 5, reuseAddress = True, **kwargs ):
    osSocket = socket.socket( socket.AF_INET, socket.SOCK_STREAM )
    if reuseAddress:
      osSocket.setsockopt( socket.SOL_SOCKET, socket.SO_REUSEADDR, 1 )
    socketInfo = self.generateServerInfo( kwargs )
    sslSocket = GSI.SSL.Connection( socketInfo.getSSLContext(), osSocket )
    sslSocket.bind( hostAddress )
    sslSocket.listen( listeningQueueSize )
    socketInfo.setSSLSocket( sslSocket )
    return socketInfo

gSocketInfoFactory = SocketInfoFactory()