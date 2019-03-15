"""
This module seemd to be used only by HTTPDISETConnection, which was removed in v7r0.
It will be removed in v7r1

"""
# $HeadURL$
__RCSID__ = "$Id$"

import types
from DIRAC import S_OK, S_ERROR, gConfig
from DIRAC.Core.DISET.private.Transports.SSL.FakeSocket import FakeSocket
from DIRAC.Core.DISET.private.Transports.SSL.pygsi.SocketInfoFactory import gSocketInfoFactory
from DIRAC.Core.DISET.private.Transports.SSLTransport import checkSanity

class SSLSocketFactory:

  KW_USE_CERTIFICATES = "useCertificates"
  KW_PROXY_LOCATION = "proxyLocation"
  KW_PROXY_STRING = "proxyString"
  KW_PROXY_CHAIN = "proxyChain"
  KW_SKIP_CA_CHECK = "skipCACheck"
  KW_TIMEOUT = "timeout"
  KW_ENABLE_SESSIONS = 'enableSessions'
  KW_SSL_METHOD = "sslMethod"
  KW_SSL_CIPHERS = "sslCiphers"

  def __checkKWArgs( self, kwargs ):
    for arg, value in ( ( self.KW_TIMEOUT, False ),
                        ( self.KW_ENABLE_SESSIONS, True ),
                        ( self.KW_SSL_METHOD, "TLSv1" ) ):
      if arg not in kwargs:
        kwargs[ arg ] = value

  def createClientSocket( self, addressTuple , **kwargs ):
    if type( addressTuple ) not in ( types.ListType, types.TupleType ):
      return S_ERROR( "hostAdress is not in a tuple form ( 'hostnameorip', port )" )
    res = gConfig.getOptionsDict( "/DIRAC/ConnConf/%s:%s" % addressTuple[0:2] )
    if res[ 'OK' ]:
      opts = res[ 'Value' ]
      for k in opts:
        if k not in kwargs:
          kwargs[k] = opts[k]
    self.__checkKWArgs( kwargs )
    result = checkSanity( addressTuple, kwargs )
    if not result[ 'OK' ]:
      return result
    result = gSocketInfoFactory.getSocket( addressTuple, **kwargs )
    if not result[ 'OK' ]:
      return result
    socketInfo = result[ 'Value' ]
    return S_OK( FakeSocket( socketInfo.getSSLSocket(), copies = 1 ) )

gSSLSocketFactory = SSLSocketFactory()
