# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/DISET/private/BaseClient.py,v 1.31 2008/03/05 10:53:59 acasajus Exp $
__RCSID__ = "$Id: BaseClient.py,v 1.31 2008/03/05 10:53:59 acasajus Exp $"

import sys
import DIRAC
from DIRAC.Core.DISET.private.Protocols import gProtocolDict
from DIRAC.LoggingSystem.Client.Logger import gLogger
from DIRAC.Core.Utilities import List, Network
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Config import gConfig
from DIRAC.ConfigurationSystem.Client.PathFinder import *
from DIRAC.Core.Utilities import GridCredentials

class BaseClient:

  defaultHostGroup = "hosts"

  KW_USE_CERTIFICATES = "useCertificates"
  KW_GROUP_TO_USE = "groupToUse"
  KW_TIMEOUT = "timeout"
  KW_SETUP = "setup"
  KW_DELEGATED_DN = "delegatedDN"
  KW_DELEGATED_GROUP = "delegatedGroup"
  KW_IGNORE_GATEWAYS = "ignoreGateways"
  KW_PROXY_LOCATION = "proxyLocation"

  def __init__( self, serviceName, **kwargs ):
    self.defaultUserGroup = gConfig.getValue( '/DIRAC/DefaultGroup', 'lhcb_user' )
    self.serviceName = serviceName
    self.kwargs = kwargs
    self.__discoverSetup()
    self.__discoverURL()
    self.__discoverTimeout()
    self.__discoverCredentialsToUse()
    self.__discoverGroup()
    self.__checkTransportSanity()

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
    if retVal[ 'OK' ]:
      self.URLTuple = retVal[ 'Value' ]
    else:
      gLogger.error( "URL is malformed", retVal[ 'Message' ] )
      raise Exception( retVal[ 'Message' ] )

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

  def __discoverGroup( self ):
    #Wich group to use?
    if self.KW_GROUP_TO_USE in self.kwargs:
      self.groupToUse = self.kwargs[ self.KW_GROUP_TO_USE ]
    else:
      if self.useCertificates:
        self.groupToUse = self.defaultHostGroup
      else:
        self.groupToUse = GridCredentials.getDIRACGroup( self.defaultUserGroup )
    #Are we delegating something?
    if self.KW_DELEGATED_DN in self.kwargs:
      if self.KW_DELEGATED_GROUP in self.kwargs:
        self.groupToUse = self.kwargs[ self.KW_DELEGATED_GROUP ]
      self.groupToUse = ( self.kwargs[ self.KW_DELEGATED_DN ], self.groupToUse )

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

  def _connect( self ):
    gLogger.debug( "Connecting to: %s" % self.serviceURL )
    try:
      self.transport = gProtocolDict[ self.URLTuple[0] ][0]( self.URLTuple[1:3], **self.kwargs )
      self.transport.initAsClient()
    except Exception, e:
      return S_ERROR( "Can't connect: %s" % str( e ) )
    return S_OK()

  def _proposeAction( self, sAction ):
    stConnectionInfo = ( ( self.URLTuple[3], self.setup ), sAction, self.groupToUse )
    self.transport.sendData( S_OK( stConnectionInfo ) )
    return self.transport.receiveData()

  def __checkTransportSanity( self ):
    retVal = gProtocolDict[ self.URLTuple[0] ][1]( self.URLTuple[1:3], **self.kwargs )
    if not retVal:
      DIRAC.abort( 10, "Insane environment for protocol" )

  def __nonzero__( self ):
    return True