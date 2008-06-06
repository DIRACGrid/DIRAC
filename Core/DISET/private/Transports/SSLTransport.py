# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/DISET/private/Transports/SSLTransport.py,v 1.20 2008/06/06 12:32:04 acasajus Exp $
__RCSID__ = "$Id: SSLTransport.py,v 1.20 2008/06/06 12:32:04 acasajus Exp $"

import os
import types
from DIRAC.Core.DISET.private.Transports.BaseTransport import BaseTransport
from DIRAC.LoggingSystem.Client.Logger import gLogger
from DIRAC.Core.DISET.private.Transports.SSL.SocketInfoFactory import gSocketInfoFactory
from DIRAC.Core import Security

class SSLTransport( BaseTransport ):

  def initAsClient( self ):
    self.oSocketInfo = gSocketInfoFactory.getSocket( self.stServerAddress, **self.extraArgsDict )
    self.oSocket = self.oSocketInfo.getSSLSocket()
    if not self.oSocket.session_reused():
      gLogger.debug( "New session connecting to server at %s" % str( self.stServerAddress ) )

  def initAsServer( self ):
    if not self.serverMode():
      raise RuntimeError( "Must be initialized as server mode" )
    self.oSocketInfo = gSocketInfoFactory.getListeningSocket( self.stServerAddress,
                                                      self.iListenQueueSize,
                                                      self.bAllowReuseAddress,
                                                      **self.extraArgsDict )
    self.oSocket = self.oSocketInfo.getSSLSocket()

  def close( self ):
    gLogger.debug( "Closing socket" )
    self.oSocket.shutdown()
    self.oSocket.close()

  def handshake( self ):
    creds = self.oSocketInfo.doServerHandshake()
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
    oClientTransport.oSocketInfo = self.oSocketInfo.clone()
    oClientTransport.setClientSocket( oClientSocket )
    return oClientTransport


def checkSanity( urlTuple, kwargs ):
  """
  Check that all ssl environment is ok
  """
  useCerts = False
  if not Security.Locations.getCAsLocation():
    gLogger.error( "No CAs found!" )
    return False
  if "useCertificates" in kwargs and kwargs[ 'useCertificates' ]:
    certTuple = Security.Locations.getHostCertificateAndKeyLocation()
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
      certFile = Security.Locations.getProxyLocation()
    if not certFile:
      gLogger.error( "No proxy found" )
      return False
    elif not os.path.isfile( certFile ):
      gLogger.error( "%s proxy file does not exist" % certFile )
      return False

  if "proxyString" in kwargs:
    certObj = Security.X509Chain()
    retVal = certObj.loadChainFromString( kwargs[ 'proxyString' ] )
    if not retVal[ 'OK' ]:
      gLogger.error( "Can't load proxy string" )
      return False
  else:
    if useCerts:
      certObj = Security.X509Certificate()
      certObj.loadFromFile( certFile )
    else:
      certObj = Security.X509Chain()
      certObj.loadChainFromFile( certFile )

  retVal = certObj.isExpired()
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
    chain = Security.X509Chain()
    certTuple = Security.getHostCertificateAndKeyLocation()
    chain.loadChainFromFile( certTuple[0] )
    chain.loadKeyFromFile( certTuple[1] )
  elif "proxyObject" in kwargs:
    chain = kwargs[ 'proxyObject' ]
  else:
    if "proxyLocation" in kwargs:
      procLoc = kwargs[ "proxyLocation" ]
    else:
      procLoc = Security.getProxyLocation()
    chain = Security.X509Chain()
    chain.loadChainFromFile( procLoc )
    chain.loadKeyFromFile( procLoc )
  return chain.generateChainFromRequestString( delegationRequest )
