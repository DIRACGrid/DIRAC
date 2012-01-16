# $HeadURL$
__RCSID__ = "$Id$"

import httplib
import socket
import time
import GSI
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR
from DIRAC.Core.DISET.private.SSLSocketFactory import gSSLSocketFactory
from DIRAC.ConfigurationSystem.Client.Config import gConfig

class HTTPDISETSocket:

  def __init__( self, gsiSocket ):
    self.gsiSocket = gsiSocket
    self.instances = 1
    self._storedBufer = ""

  def makefile( self, mode, bs ):
    self.instances += 1
    return self

  def close( self ):
    self.instances -= 1
    if self.instances == 0:
      self.gsiSocket.shutdown()
      self.gsiSocket.close()

  def read( self, bytes ):
    while len( self._storedBufer ) < bytes:
      result = self._read( bytes - len( self._storedBufer ) )
      if not result[ 'OK' ]:
        break
        #raise Exception( result[ 'Message' ] )
      else:
        data = result[ 'Value' ]
        if not data:
          break
        self._storedBufer += data
    data = self._storedBufer[:bytes]
    self._storedBufer = self._storedBufer[bytes:]
    return data

  def _read( self, bufSize = 4096 ):
    start = time.time()
    timeout = 0
    while True:
      if timeout:
        if time.time() - start > timeout:
          return S_ERROR( "Socket read timeout exceeded" )
      try:
        data = self.gsiSocket.recv( bufSize )
        return S_OK( data )
      except GSI.SSL.WantReadError:
        time.sleep( 0.001 )
      except GSI.SSL.WantWriteError:
        time.sleep( 0.001 )
      except GSI.SSL.ZeroReturnError:
        return S_OK( "" )
      except Exception, e:
        return S_ERROR( "Exception while reading from peer: %s" % str( e ) )

  def readline( self ):
    buf = ""
    sepPos = self._storedBufer.find( "\n" )
    while sepPos == -1 :
      result = self._read( 128 )
      if not result[ 'OK' ]:
        raise Exception( result[ 'Message' ] )
      data = result[ 'Value' ]
      if not data:
        break
      self._storedBufer += data
      sepPos = self._storedBufer.find( "\n" )
    if sepPos == -1:
      line = self._storedBufer
      self._storedBufer = ""
    else:
      line = self._storedBufer[:sepPos + 1]
      self._storedBufer = self._storedBufer[sepPos + 1:]
    return line

class HTTPDISETResponse( httplib.HTTPResponse ):

  def __init__( self, sock, *args, **kwargs ):
    httplib.HTTPResponse.__init__( self, HTTPDISETSocket( sock ), *args, **kwargs )

class HTTPDISETConnection( httplib.HTTPConnection ):

  response_class = HTTPDISETResponse

  def __init__( self, *args, **kwargs ):
    httplib.HTTPConnection.__init__( self, *args, **kwargs )
    self.set_debuglevel( 10 )

  def connect( self ):
    errorMsg = ""
    for res in socket.getaddrinfo( self.host, self.port, 0,
                                  socket.SOCK_STREAM ):
      af, socktype, proto, canonname, addTuple = res
      result = gSSLSocketFactory.createClientSocket( addTuple, useCertificates = gConfig._useServerCertificate() )
      if not result[ 'OK' ]:
        errorMsg = result[ 'Message' ]
        continue
      self.sock = result[ 'Value' ]
      break
    if not self.sock:
      raise socket.error, errorMsg




