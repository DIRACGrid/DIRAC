# $HeadURL$
__RCSID__ = "$Id$"

import socket
import select
import os
try:
  import hashlib as md5
except:
  import md5
import GSI
from DIRAC.Core.Utilities.ReturnValues import S_ERROR, S_OK
from DIRAC.Core.Utilities import List, Network
from DIRAC.Core.DISET.private.Transports.SSL.SocketInfo import SocketInfo
from DIRAC.Core.DISET.private.Transports.SSL.SessionManager import gSessionManager
from DIRAC.Core.DISET.private.Transports.SSL.FakeSocket import FakeSocket
from DIRAC.Core.DISET.private.Transports.SSL.ThreadSafeSSLObject import ThreadSafeSSLObject

if GSI.__version__ < "0.5.0":
  raise Exception( "Required GSI version >= 0.5.0" )

class SocketInfoFactory:

  def generateClientInfo( self, destinationHostname, kwargs ):
    infoDict = { 'clientMode' : True,
                 'hostname' : destinationHostname,
                 'timeout' : 600,
                 'enableSessions' : True }
    for key in kwargs.keys():
      infoDict[ key ] = kwargs[ key ]
    try:
      return S_OK( SocketInfo( infoDict ) )
    except Exception, e:
      return S_ERROR( "Error while creating SSL context: %s" % str( e ) )

  def generateServerInfo( self, kwargs ):
    infoDict = { 'clientMode' : False, 'timeout' : 30 }
    for key in kwargs.keys():
      infoDict[ key ] = kwargs[ key ]
    try:
      return S_OK( SocketInfo( infoDict ) )
    except Exception, e:
      return S_ERROR( str( e ) )

  def __socketConnect( self, hostAddress, timeout, retries = 2 ):
    osSocket = socket.socket( socket.AF_INET, socket.SOCK_STREAM )
    #osSocket.setblocking( 0 )
    if timeout:
      osSocket.settimeout( 5 )
    try:
      osSocket.connect( hostAddress )
    except socket.error , e:
      if e.args[0] == "timed out":
        osSocket.close()
        if retries:
          return self.__socketConnect( hostAddress, timeout, retries - 1 )
        else:
          return S_ERROR( "Can't connect: %s" % str( e ) )
      if e.args[0] not in ( 114, 115 ):
        return S_ERROR( "Can't connect: %s" % str( e ) )
      #Connect in progress
      oL = select.select( [], [ osSocket ], [], timeout )[1]
      if len( oL ) == 0:
        osSocket.close()
        return S_ERROR( "Connection timeout" )
      errno = osSocket.getsockopt( socket.SOL_SOCKET, socket.SO_ERROR )
      if errno != 0:
        return S_ERROR( "Can't connect: %s" % str( ( errno, os.strerror( errno ) ) ) )
    return S_OK( osSocket )

  def __connect( self, socketInfo, hostAddress ):
    #Connect baby!
    result = self.__socketConnect( hostAddress, socketInfo.infoDict[ 'timeout' ] )
    if not result[ 'OK' ]:
      return result
    osSocket = result[ 'Value' ]
    #SSL MAGIC
    sslSocket = GSI.SSL.Connection( socketInfo.getSSLContext(), osSocket )
    #Generate sessionId
    sessionHash = md5.md5()
    sessionHash.update( str( hostAddress ) )
    sessionHash.update( "|%s" % str( socketInfo.getLocalCredentialsLocation() ) )
    for key in ( 'proxyLocation', 'proxyString' ):
      if key in socketInfo.infoDict:
        sessionHash.update( "|%s" % str( socketInfo.infoDict[ key ] ) )
    if 'proxyChain' in socketInfo.infoDict:
      sessionHash.update( "|%s" % socketInfo.infoDict[ 'proxyChain' ].dumpAllToString()[ 'Value' ] )
    sessionId = sessionHash.hexdigest()
    socketInfo.sslContext.set_session_id( str( hash( sessionId ) ) )
    socketInfo.setSSLSocket( sslSocket )
    if gSessionManager.isValid( sessionId ):
      sslSocket.set_session( gSessionManager.get( sessionId ) )
    #Set the real timeout
    if socketInfo.infoDict[ 'timeout' ]:
      sslSocket.settimeout( socketInfo.infoDict[ 'timeout' ] )
    #Connected!
    return S_OK( sslSocket )

  def getSocket( self, hostAddress, **kwargs ):
    hostName = hostAddress[0]
    retVal = self.generateClientInfo( hostName, kwargs )
    if not retVal[ 'OK' ]:
      return retVal
    socketInfo = retVal[ 'Value' ]
    retVal = Network.getIPsForHostName( hostName )
    if not retVal[ 'OK' ]:
      return S_ERROR( "Could not resolve %s: %s" % ( hostName, retVal[ 'Message' ] ) )
    ipList = List.randomize( retVal[ 'Value' ] )
    for i in range( 3 ):
      connected = False
      errorsList = []
      for ip in ipList :
        ipAddress = ( ip, hostAddress[1] )
        retVal = self.__connect( socketInfo, ipAddress )
        if retVal[ 'OK' ]:
          sslSocket = retVal[ 'Value' ]
          connected = True
          break
        errorsList.append( "%s: %s" % ( ipAddress, retVal[ 'Message' ] ) )
      if not connected:
        return S_ERROR( "Could not connect to %s: %s" % ( hostAddress, "," .join( [ e for e in errorsList ] ) ) )
      retVal = socketInfo.doClientHandshake()
      if retVal[ 'OK' ]:
        #Everything went ok. Don't need to retry
        break
    #Did the auth or the connection fail?
    if not retVal['OK']:
      return retVal
    if 'enableSessions' in kwargs and kwargs[ 'enableSessions' ]:
      sessionId = hash( hostAddress )
      gSessionManager.set( sessionId, sslSocket.get_session() )
    return S_OK( socketInfo )

  def getListeningSocket( self, hostAddress, listeningQueueSize = 5, reuseAddress = True, **kwargs ):
    osSocket = socket.socket( socket.AF_INET, socket.SOCK_STREAM )
    if reuseAddress:
      osSocket.setsockopt( socket.SOL_SOCKET, socket.SO_REUSEADDR, 1 )
    retVal = self.generateServerInfo( kwargs )
    if not retVal[ 'OK' ]:
      return retVal
    socketInfo = retVal[ 'Value' ]
    sslSocket = GSI.SSL.Connection( socketInfo.getSSLContext(), osSocket )
    sslSocket.bind( hostAddress )
    sslSocket.listen( listeningQueueSize )
    socketInfo.setSSLSocket( sslSocket )
    return S_OK( socketInfo )

  def renewServerContext( self, origSocketInfo ):
    retVal = self.generateServerInfo( origSocketInfo.infoDict )
    if not retVal[ 'OK' ]:
      return retVal
    socketInfo = retVal[ 'Value' ]
    osSocket = origSocketInfo.getSSLSocket().get_socket()
    sslSocket = GSI.SSL.Connection( socketInfo.getSSLContext(), osSocket )
    socketInfo.setSSLSocket( sslSocket )
    return S_OK( socketInfo )

gSocketInfoFactory = SocketInfoFactory()
