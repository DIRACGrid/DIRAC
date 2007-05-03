
import time
import copy
from OpenSSL import SSL
from DIRAC.Core.Utilities import GridCert
from DIRAC.LoggingSystem.Client.Logger import gLogger

class SocketInfo:

  def __init__( self, infoDict ):
    self.infoDict = infoDict
    if self.infoDict[ 'clientMode' ]:
      self.__generateClientContext()
    else:
      self.__generateServerContext()

  def setLocalCredentialsLocation( self, credTuple ):
    self.infoDict[ 'localCredentialsLocation' ] = credTuple

  def getLocalCredentialsLocation( self ):
    return self.infoDict[ 'localCredentialsLocation' ]

  def gatherPeerCredentials( self ):
    peerCert = self.sslSocket.get_peer_certificate()
    peerDN = self.__cleanDN( peerCert.get_subject() )
    credDict = { 'certificate' : peerCert, 'DN' : peerDN }
    self.infoDict[ 'peerCredentials' ] = credDict
    return credDict

  def __cleanDN( self, dn ):
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
    return SocketInfo( copy.deepcopy( self.infoDict ) )

  def verifyCallback( self, *args, **kwargs ):
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
    gLogger.verbose( "CAs location is %s" % casPath )
    self.sslContext.load_verify_locations_path( casPath )
    self.sslContext.set_session_id( "DISETConnection%s" % str( time.time() ) )

  def __generateServerContext( self ):
    self.__createContext()
    certKeyTuple = GridCert.getCertificateAndKey()
    gLogger.verbose("Using certificate %s\nUsing key %s" % certKeyTuple )
    self.sslContext.use_certificate_chain_file( certKeyTuple[0] )
    self.sslContext.use_privatekey_file(  certKeyTuple[1] )
    self.setLocalCredentialsLocation( certKeyTuple )

  def __generateClientContext( self ):
    self.__createContext()
    proxyPath = GridCert.getGridProxy()
    gLogger.verbose( "Using proxy %s" % proxyPath )
    self.sslContext.use_certificate_chain_file( proxyPath )
    self.sslContext.use_privatekey_file( proxyPath )
    self.setLocalCredentialsLocation( ( proxyPath, proxyPath ) )

  def doClientHandshake( self ):
    self.sslSocket.set_connect_state()
    return self.__sslHandshake()

  def doServerHandshake( self ):
    self.sslSocket.set_accept_state()
    return self.__sslHandshake()

  def __sslHandshake( self ):
    try:
      self.sslSocket.do_handshake()
    except SSL.Error, v:
      #FIXME: S_ERROR?
      #gLogger.warn( "Error while handshaking", "\n".join( [ stError[2] for stError in v.args[0] ] ) )
      gLogger.warn( "Error while handshaking", v )
      raise
    self.gatherPeerCredentials()
    gLogger.info( "", "Authorized peer (%s)" % credetialsDict[ 'DN' ] )
    return credetialsDict
