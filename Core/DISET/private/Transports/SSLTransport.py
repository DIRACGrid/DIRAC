# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/DISET/private/Transports/SSLTransport.py,v 1.14 2008/03/13 10:02:32 acasajus Exp $
__RCSID__ = "$Id: SSLTransport.py,v 1.14 2008/03/13 10:02:32 acasajus Exp $"

import os
from DIRAC.Core.DISET.private.Transports.BaseTransport import BaseTransport
from DIRAC.LoggingSystem.Client.Logger import gLogger
from DIRAC.Core.DISET.private.Transports.SSL.SocketInfoFactory import gSocketInfoFactory
from DIRAC.Core.Utilities import GridCredentials

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


def checkSanity( *args, **kwargs ):
  """
  Check that all ssl environment is ok
  """
  saneEnv = True
  if not GridCredentials.getCAsLocation():
    gLogger.fatal( "No CAs found!" )
    saneEnv = False
  if "useCertificates" in kwargs and kwargs[ 'useCertificates' ]:
    certTuple = GridCredentials.getHostCertificateAndKey()
    if not certTuple:
      gLogger.fatal( "No cert/key found! " )
      saneEnv = False
    else:
      certFile = certTuple[0]
  else:
    if "proxyLocation" in kwargs:
      certFile = kwargs[ "proxyLocation" ]
    else:
      certFile = GridCredentials.getGridProxy()
    if not certFile:
      gLogger.fatal( "No proxy found" )
      saneEnv = False
    elif not os.path.isfile( certFile ):
      gLogger.fatal( "%s proxy file does not exist" % certFile )
      saneEnv = False

  if saneEnv:
    certObj = GridCredentials.X509Certificate()
    certObj.loadFromFile( certFile )
    retVal = certObj.isExpired()
    if not retVal[ 'OK' ]:
      gLogger.fatal( "Can't verify file %s:%s" % ( certFile, retVal[ 'Message' ] ) )
      saneEnv = False
    else:
      if retVal[ 'Value' ]:
        gLogger.fatal( "PEM file %s has expired, not valid after %s" % ( certFile, certObj.getNotAfterDate() ) )
        saneEnv = False

  return saneEnv

