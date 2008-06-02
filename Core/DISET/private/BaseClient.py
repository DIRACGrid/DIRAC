# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/DISET/private/BaseClient.py,v 1.36 2008/06/02 13:28:38 acasajus Exp $
__RCSID__ = "$Id: BaseClient.py,v 1.36 2008/06/02 13:28:38 acasajus Exp $"

import sys
import types
import thread
import DIRAC
from DIRAC.Core.DISET.private.Protocols import gProtocolDict
from DIRAC.LoggingSystem.Client.Logger import gLogger
from DIRAC.Core.Utilities import List, Network
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Config import gConfig
from DIRAC.ConfigurationSystem.Client.PathFinder import *
from DIRAC.Core.Utilities import GridCredentials

class BaseClient:

  __defaultHostExtraCredentials = "hosts"

  KW_USE_CERTIFICATES = "useCertificates"
  KW_EXTRA_CREDENTIALS = "extraCredentials"
  KW_TIMEOUT = "timeout"
  KW_SETUP = "setup"
  KW_DELEGATED_DN = "delegatedDN"
  KW_DELEGATED_GROUP = "delegatedGroup"
  KW_IGNORE_GATEWAYS = "ignoreGateways"
  KW_PROXY_LOCATION = "proxyLocation"
  KW_PROXY_OBJECT = "proxyObject"

  def __init__( self, serviceName, **kwargs ):
    if type( serviceName ) != types.StringType:
      raise TypeError( "Service name expected to be a string. Received %s type %s" % ( str(serviceName), type(serviceName) ) )
    self.serviceName = serviceName
    self.kwargs = kwargs
    self.__extraCredentials = False
    self.__discoverSetup()
    self.__initStatus = self.__discoverURL()
    if not self.__initStatus[ 'OK' ]:
      return
    self.__discoverTimeout()
    self.__discoverCredentialsToUse()
    self.__discoverExtraCredentials()
    self.__initStatus = self.__checkTransportSanity()
    #HACK for thread-safety:
    self.__allowedThreadID = False

  def __discoverSetup(self):
    #Which setup to use?
    if self.KW_SETUP in self.kwargs:
      self.setup = str( self.kwargs[ self.KW_SETUP ] )
    else:
      self.setup = gConfig.getValue( "/DIRAC/Setup", "LHCb-Development" )

  def __discoverURL(self):
    #Calculate final URL
    self.serviceURL = self.__findServiceURL()
    retVal = Network.splitURL( self.serviceURL )
    if not retVal[ 'OK' ]:
      return S_ERROR( "URL is malformed: %s" % retVal[ 'Message' ] )
    self.URLTuple = retVal[ 'Value' ]
    return S_OK()

  def __discoverTimeout( self ):
    if self.KW_TIMEOUT in self.kwargs:
      self.timeout = self.kwargs[ self.KW_TIMEOUT ]
    else:
      self.timeout = False
    #HACK: For windows there is no timeout! (YingYing...)
    if sys.platform == "win32":
      self.timeout = False

  def __discoverCredentialsToUse( self ):
    #Use certificates?
    if self.KW_USE_CERTIFICATES in self.kwargs:
      self.useCertificates = self.kwargs[ self.KW_USE_CERTIFICATES ]
    else:
      self.useCertificates = gConfig._useServerCertificate()
      self.kwargs[ self.KW_USE_CERTIFICATES ] = self.useCertificates

  def __discoverExtraCredentials( self ):
    #Wich extra credentials to use?
    if self.KW_EXTRA_CREDENTIALS in self.kwargs:
      self.__extraCredentials = self.kwargs[ self.KW_EXTRA_CREDENTIALS ]
    elif self.useCertificates:
        self.__extraCredentials = self.__defaultHostExtraCredentials
    #Are we delegating something?
    if self.KW_DELEGATED_DN in self.kwargs:
      if self.KW_DELEGATED_GROUP in self.kwargs:
        self.__extraCredentials = self.kwargs[ self.KW_DELEGATED_GROUP ]
      self.__extraCredentials = ( self.kwargs[ self.KW_DELEGATED_DN ], self.__extraCredentials )

  def __findServiceURL( self ):
    for protocol in gProtocolDict.keys():
      if self.serviceName.find( "%s://" % protocol ) == 0:
        gLogger.debug( "Already given a valid url", self.serviceName )
        return self.serviceName

    if self.KW_IGNORE_GATEWAYS not in self.kwargs or not self.kwargs[ self.KW_IGNORE_GATEWAYS ]:
      dRetVal = gConfig.getOption( "/LocalSite/Site" )
      if dRetVal[ 'OK' ]:
        siteName = dRetVal[ 'Value' ]
        dRetVal = gConfig.getOption( "/DIRAC/Gateways/%s" % siteName )
        if dRetVal[ 'OK' ]:
          rawGatewayURL = List.randomize( List.fromChar( dRetVal[ 'Value'], "," ) )[0]
          gatewayURL = "/".join( rawGatewayURL.split( "/" )[:3] )
          gLogger.debug( "Using gateway", gatewayURL )
          return "%s/%s" % (  gatewayURL, self.serviceName )

    urls = getServiceURL( self.serviceName, setup = self.setup )
    if not urls:
      raise Exception( "URL for service %s not found" % self.serviceName )
    sURL = List.randomize( List.fromChar( urls, "," ) )[0]
    gLogger.debug( "Discovering URL for service", "%s -> %s" % ( self.serviceName, sURL ) )
    return sURL

  def __checkThreadID( self ):
    cThID = thread.get_ident()
    if not self.__allowedThreadID:
      self.__allowedThreadID = cThID
    elif cThID != self.__allowedThreadID :
      msgTxt = """
=======DISET client thread safety error========================
Client %s
can only run on thread %s
and this is thread %s
===============================================================""" % ( str( self ),
                                                                         self.__allowedThreadID,
                                                                         cThID  )
      gLogger.error( msgTxt )
      #raise Exception( msgTxt )


  def _connect( self ):
    self.__checkThreadID()
    gLogger.debug( "Connecting to: %s" % self.serviceURL )
    if not self.__initStatus[ 'OK' ]:
      return self.__initStatus
    try:
      transport = gProtocolDict[ self.URLTuple[0] ][0]( self.URLTuple[1:3], **self.kwargs )
      transport.initAsClient()
    except Exception, e:
      return S_ERROR( "Can't connect: %s" % str( e ) )
    return S_OK( transport )

  def _proposeAction( self, transport, sAction ):
    stConnectionInfo = ( ( self.URLTuple[3], self.setup ), sAction, self.__extraCredentials )
    transport.sendData( S_OK( stConnectionInfo ) )
    serverReturn = transport.receiveData()
    #TODO: Check if delegation is required
    return serverReturn

  def __checkTransportSanity( self ):
    saneEnv = gProtocolDict[ self.URLTuple[0] ][1]( self.URLTuple[1:3], **self.kwargs )
    if not saneEnv:
      return S_ERROR( "Insane environment for protocol" )
    return S_OK()

  def __nonzero__( self ):
    return True

  def __str__( self ):
    return "<DISET Client %s %s>" % ( self.serviceURL, self.__extraCredentials )