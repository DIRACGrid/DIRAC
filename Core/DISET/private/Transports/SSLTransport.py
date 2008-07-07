# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/DISET/private/Transports/SSLTransport.py,v 1.23 2008/07/07 16:37:19 acasajus Exp $
__RCSID__ = "$Id: SSLTransport.py,v 1.23 2008/07/07 16:37:19 acasajus Exp $"

import os
import types
from DIRAC.Core.Utilities.ReturnValues import S_ERROR, S_OK
from DIRAC.Core.DISET.private.Transports.BaseTransport import BaseTransport
from DIRAC.LoggingSystem.Client.Logger import gLogger
from DIRAC.Core.DISET.private.Transports.SSL.SocketInfoFactory import gSocketInfoFactory
from DIRAC.Core.Security import Locations
from DIRAC.Core.Security.X509Chain import X509Chain
from DIRAC.Core.Security.X509Certificate import X509Certificate

class SSLTransport( BaseTransport ):

  def initAsClient( self ):
    retVal = gSocketInfoFactory.getSocket( self.stServerAddress, **self.extraArgsDict )
    if not retVal[ 'OK' ]:
      return retVal
    self.oSocketInfo = retVal[ 'Value' ]
    self.oSocket = self.oSocketInfo.getSSLSocket()
    if not self.oSocket.session_reused():
      gLogger.debug( "New session connecting to server at %s" % str( self.stServerAddress ) )
    return S_OK()

  def initAsServer( self ):
    if not self.serverMode():
      raise RuntimeError( "Must be initialized as server mode" )
    retVal = gSocketInfoFactory.getListeningSocket( self.stServerAddress,
                                                      self.iListenQueueSize,
                                                      self.bAllowReuseAddress,
                                                      **self.extraArgsDict )
    if not retVal[ 'OK' ]:
      return retVal
    self.oSocketInfo = retVal[ 'Value' ]
    self.oSocket = self.oSocketInfo.getSSLSocket()
    return S_OK()

  def close( self ):
    gLogger.debug( "Closing socket" )
    self.oSocket.shutdown()
    self.oSocket.close()

  def handshake( self ):
    retVal = self.oSocketInfo.doServerHandshake()
    if not retVal[ 'OK' ]:
      return retVal
    creds = retVal[ 'Value' ]
    if not self.oSocket.session_reused():
      gLogger.debug( "New session connecting from client at %s" % str( self.getRemoteAddress() ) )
    for key in creds.keys():
      self.peerCredentials[ key ] = creds[ key ]

  def setClientSocket( self, oSocket ):
    if self.serverMode():
      raise RuntimeError( "Must be initialized as client mode" )
    self.oSocketInfo.setSSLSocket( oSocket )
    self.oSocket = oSocket

  def acceptConnection( self ):
    oClientTransport = SSLTransport( self.stServerAddress )
    oClientSocket, stClientAddress = self.oSocket.accept()
    retVal = self.oSocketInfo.clone()
    if not retVal[ 'OK' ]:
      return retVal
    oClientTransport.oSocketInfo = retVal[ 'Value' ]
    oClientTransport.setClientSocket( oClientSocket )
    return S_OK( oClientTransport )


def checkSanity( urlTuple, kwargs ):
  """
  Check that all ssl environment is ok
  """
  useCerts = False
  if not Locations.getCAsLocation():
    gLogger.error( "No CAs found!" )
    return False
  if "useCertificates" in kwargs and kwargs[ 'useCertificates' ]:
    certTuple = Locations.getHostCertificateAndKeyLocation()
    if not certTuple:
      gLogger.error( "No cert/key found! " )
      return False
    certFile = certTuple[0]
    useCerts = True
  elif "proxyString" in kwargs:
    if type( kwargs[ 'proxyString' ] ) != types.StringType:
      gLogger.error( "proxyString parameter is not a valid type" )
      return False
  else:
    if "proxyLocation" in kwargs:
      certFile = kwargs[ "proxyLocation" ]
    else:
      certFile = Locations.getProxyLocation()
    if not certFile:
      gLogger.error( "No proxy found" )
      return False
    elif not os.path.isfile( certFile ):
      gLogger.error( "%s proxy file does not exist" % certFile )
      return False

  if "proxyString" in kwargs:
    certObj = X509Chain()
    retVal = certObj.loadChainFromString( kwargs[ 'proxyString' ] )
    if not retVal[ 'OK' ]:
      gLogger.error( "Can't load proxy string" )
      return False
  else:
    if useCerts:
      certObj = X509Certificate()
      certObj.loadFromFile( certFile )
    else:
      certObj = X509Chain()
      certObj.loadChainFromFile( certFile )

  retVal = certObj.hasExpired()
  if not retVal[ 'OK' ]:
    gLogger.error( "Can't verify file %s:%s" % ( certFile, retVal[ 'Message' ] ) )
    return False
  else:
    if retVal[ 'Value' ]:
      notAfter = certObj.getNotAfterDate()
      if notAfter[ 'OK' ]:
        notAfter = notAfter[ 'Value' ]
      else:
        notAfter = "unknown"
      gLogger.error( "PEM file %s has expired, not valid after %s" % ( certFile, notAfter ) )
      return False

  return True

def delegate( delegationRequest, kwargs ):
  """
  Check delegate!
  """
  if "useCertificates" in kwargs and kwargs[ 'useCertificates' ]:
    chain = X509Chain()
    certTuple = Locations.getHostCertificateAndKeyLocation()
    chain.loadChainFromFile( certTuple[0] )
    chain.loadKeyFromFile( certTuple[1] )
  elif "proxyObject" in kwargs:
    chain = kwargs[ 'proxyObject' ]
  else:
    if "proxyLocation" in kwargs:
      procLoc = kwargs[ "proxyLocation" ]
    else:
      procLoc = Locations.getProxyLocation()
    chain = X509Chain()
    chain.loadChainFromFile( procLoc )
    chain.loadKeyFromFile( procLoc )
  return chain.generateChainFromRequestString( delegationRequest )
