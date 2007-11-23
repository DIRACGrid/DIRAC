# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/DISET/private/Transports/SSL/SocketInfo.py,v 1.12 2007/11/23 13:34:28 acasajus Exp $
__RCSID__ = "$Id: SocketInfo.py,v 1.12 2007/11/23 13:34:28 acasajus Exp $"

import time
import copy
import threading
from OpenSSL import SSL, crypto
import DIRAC
from DIRAC.Core.Utilities import GridCert
from DIRAC.LoggingSystem.Client.Logger import gLogger

SSL.set_thread_safe()

gHandshakeLock  = threading.Lock()

class SocketInfo:

  def __init__( self, infoDict, sslContext = False ):
    self.infoDict = infoDict
    if sslContext:
      self.sslContext = sslContext
    else:
      if self.infoDict[ 'clientMode' ]:
        if self.infoDict.has_key( 'useCertificates' ) and self.infoDict[ 'useCertificates' ]:
          self.__generateContextWithCerts()
        else:
          self.__generateContextWithProxy()
      else:
        self.__generateServerContext()

  def setLocalCredentialsLocation( self, credTuple ):
    self.infoDict[ 'localCredentialsLocation' ] = credTuple

  def getLocalCredentialsLocation( self ):
    return self.infoDict[ 'localCredentialsLocation' ]

  def gatherPeerCredentials( self ):
    peerCert = self.sslSocket.get_peer_certificate()
    peerDN = self.__cleanDN( peerCert.get_subject() )
    credDict = { 'DN' : peerDN }
    self.infoDict[ 'peerCredentials' ] = credDict
    return credDict

  def __cleanDN( self, dn ):
    dn = str( dn )
    dn = dn[ 18:-2]
    for proxyRubbish in ( "/CN=proxy", "/CN=limitedproxy" ):
      position = dn.find( proxyRubbish )
      while position > -1:
        dn = dn[ :position ]
        position = dn.find( proxyRubbish )
    return dn

  def setSSLSocket( self, sslSocket ):
    self.sslSocket = sslSocket

  def getSSLSocket( self ):
    return self.sslSocket

  def getSSLContext( self ):
    return self.sslContext

  def clone( self ):
    return SocketInfo( copy.deepcopy( self.infoDict ), self.sslContext )

  def verifyCallback( self, *args, **kwargs ):
    #gLogger.debug( "verify Callback %s" % str( args ) )
    if self.infoDict[ 'clientMode' ]:
      return self._clientCallback( *args, **kwargs )
    else:
      return self._serverCallback( *args, **kwargs )

  def _clientCallback( self, conn, cert, errnum, depth, ok ):
    # This obviously has to be updated
    if depth == 0 and ok == 1:
      hostnameCN = cert.get_subject().commonName
      if hostnameCN in ( self.infoDict[ 'hostname' ], "host/%s" % self.infoDict[ 'hostname' ]  ):
        return 1
      else:
        gLogger.warn( "Server is not who it's supposed to be",
                      "Connecting to %s and it's %s" % ( self.infoDict[ 'hostname' ], hostnameCN ) )
        return ok
    return ok

  def _serverCallback( self, conn, cert, errnum, depth, ok):
    return ok

  def __createContext( self ):
    # Initialize context
    self.sslContext = SSL.Context( SSL.SSLv23_METHOD )
    self.sslContext.set_verify( SSL.VERIFY_PEER|SSL.VERIFY_FAIL_IF_NO_PEER_CERT, self.verifyCallback ) # Demand a certificate
    casPath = GridCert.getCAsLocation()
    if not casPath:
      DIRAC.abort( 10, "No valid CAs location found" )
    gLogger.debug( "CAs location is %s" % casPath )
    self.sslContext.load_verify_locations_path( casPath )

  def __generateContextWithCerts( self ):
    certKeyTuple = GridCert.getCertificateAndKey()
    if not certKeyTuple:
      DIRAC.abort( 10, "No valid certificate or key found" )
    self.setLocalCredentialsLocation( certKeyTuple )
    gLogger.debug("Using certificate %s\nUsing key %s" % certKeyTuple )
    self.__createContext()
    self.sslContext.use_certificate_chain_file( certKeyTuple[0] )
    self.sslContext.use_privatekey_file(  certKeyTuple[1] )

  def __generateContextWithProxy( self ):
    proxyPath = GridCert.getGridProxy()
    if not proxyPath:
      DIRAC.abort( 10, "No valid proxy found" )
    self.setLocalCredentialsLocation( ( proxyPath, proxyPath ) )
    gLogger.debug( "Using proxy %s" % proxyPath )
    self.__createContext()
    self.sslContext.use_certificate_chain_file( proxyPath )
    self.sslContext.use_privatekey_file( proxyPath )

  def __generateServerContext( self ):
      self.__generateContextWithCerts()
      self.sslContext.set_session_id( "DISETConnection%s" % str( time.time() ) )
      self.sslContext.get_cert_store().set_flags( crypto.X509_CRL_CHECK )
      self.sslContext.set_GSI_verify()

  def doClientHandshake( self ):
    self.sslSocket.set_connect_state()
    return self.__sslHandshake()

  def doServerHandshake( self ):
    self.sslSocket.set_accept_state()
    return self.__sslHandshake()

  def __sslHandshake( self ):
    try:
      gHandshakeLock.acquire()
      try:
        self.sslSocket.do_handshake()
      finally:
        gHandshakeLock.release()
    except SSL.Error, v:
      #FIXME: S_ERROR?
      #gLogger.warn( "Error while handshaking", "\n".join( [ stError[2] for stError in v.args[0] ] ) )
      gLogger.warn( "Error while handshaking", v )
      raise
    credentialsDict = self.gatherPeerCredentials()
    gLogger.verbose( "", "Authenticated peer (%s)" % credentialsDict[ 'DN' ] )
    return credentialsDict
