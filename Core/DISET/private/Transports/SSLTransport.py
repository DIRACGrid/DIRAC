# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/DISET/private/Transports/SSLTransport.py,v 1.7 2007/06/26 11:33:08 acasajus Exp $
__RCSID__ = "$Id: SSLTransport.py,v 1.7 2007/06/26 11:33:08 acasajus Exp $"

from DIRAC.Core.DISET.private.Transports.BaseTransport import BaseTransport
from DIRAC.LoggingSystem.Client.Logger import gLogger
from DIRAC.Core.DISET.private.Transports.SSL.SocketInfoFactory import gSocketInfoFactory
from DIRAC.Core.Utilities import GridCert

class SSLTransport( BaseTransport ):

  def initAsClient( self ):
    self.oSocketInfo = gSocketInfoFactory.getSocket( self.stServerAddress, **self.extraArgsDict )
    self.oSocket = self.oSocketInfo.getSSLSocket()

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
    saneEnv = True
    if not GridCert.getCAsLocation():
      gLogger.fatal( "No CAs found!" )
      saneEnv = False
    if "useCertificates" in kwargs and kwargs[ 'useCertificates' ]:
      if not GridCert.getCertificateAndKey():
        gLogger.fatal( "No cert/key found! " )
        saneEnv = False
    else:
      if not GridCert.getGridProxy():
          gLogger.fatal( "No proxy found!" )
          saneEnv = False
    return saneEnv

