# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/DISET/private/Transports/SSL/SocketInfo.py,v 1.4 2007/05/15 14:49:57 acasajus Exp $
__RCSID__ = "$Id: SocketInfo.py,v 1.4 2007/05/15 14:49:57 acasajus Exp $"

import time
import copy
from OpenSSL import SSL, crypto
import DIRAC
from DIRAC.Core.Utilities import GridCert
from DIRAC.LoggingSystem.Client.Logger import gLogger

class SocketInfo:

  def __init__( self, infoDict ):
    self.infoDict = infoDict
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
    credDict = { 'certificate' : peerCert, 'DN' : peerDN }
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
    if not casPath:
      DIRAC.abort( 10, "No valid CAs location found" )
    gLogger.verbose( "CAs location is %s" % casPath )
    self.sslContext.load_verify_locations_path( casPath )
    self.sslContext.set_session_id( "DISETConnection%s" % str( time.time() ) )

  def __generateContextWithCerts( self ):
    self.__createContext()
    certKeyTuple = GridCert.getCertificateAndKey()
    if not certKeyTuple:
      DIRAC.abort( 10, "No valid certificate or key found" )
    gLogger.verbose("Using certificate %s\nUsing key %s" % certKeyTuple )
    self.sslContext.use_certificate_chain_file( certKeyTuple[0] )
    self.sslContext.use_privatekey_file(  certKeyTuple[1] )
    self.setLocalCredentialsLocation( certKeyTuple )

  def __generateContextWithProxy( self ):
    self.__createContext()
    proxyPath = GridCert.getGridProxy()
    if not proxyPath:
      DIRAC.abort( 10, "No valid proxy found" )
    gLogger.verbose( "Using proxy %s" % proxyPath )
    self.sslContext.use_certificate_chain_file( proxyPath )
    self.sslContext.use_privatekey_file( proxyPath )
    self.setLocalCredentialsLocation( ( proxyPath, proxyPath ) )

  def __generateServerContext( self ):
      self.__generateContextWithCerts()
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
      self.sslSocket.do_handshake()
    except SSL.Error, v:
      #FIXME: S_ERROR?
      #gLogger.warn( "Error while handshaking", "\n".join( [ stError[2] for stError in v.args[0] ] ) )
      gLogger.warn( "Error while handshaking", v )
      raise
    credentialsDict = self.gatherPeerCredentials()
    gLogger.info( "", "Authorized peer (%s)" % credentialsDict[ 'DN' ] )
    return credentialsDict
