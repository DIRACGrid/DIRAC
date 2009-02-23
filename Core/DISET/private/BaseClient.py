# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/DISET/private/BaseClient.py,v 1.60 2009/02/23 15:52:53 acasajus Exp $
__RCSID__ = "$Id: BaseClient.py,v 1.60 2009/02/23 15:52:53 acasajus Exp $"

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
from DIRAC.Core.Security import CS

class BaseClient:

  VAL_EXTRA_CREDENTIALS_HOST = "hosts"

  KW_USE_CERTIFICATES = "useCertificates"
  KW_EXTRA_CREDENTIALS = "extraCredentials"
  KW_TIMEOUT = "timeout"
  KW_SETUP = "setup"
  KW_DELEGATED_DN = "delegatedDN"
  KW_DELEGATED_GROUP = "delegatedGroup"
  KW_IGNORE_GATEWAYS = "ignoreGateways"
  KW_PROXY_LOCATION = "proxyLocation"
  KW_PROXY_STRING = "proxyString"
  KW_PROXY_CHAIN = "proxyChain"
  KW_SKIP_CA_CHECK = "skipCACheck"

  def __init__( self, serviceName, **kwargs ):
    if type( serviceName ) != types.StringType:
      raise TypeError( "Service name expected to be a string. Received %s type %s" % ( str(serviceName), type(serviceName) ) )
    self.serviceName = serviceName
    self.kwargs = kwargs
    self.__initStatus = S_OK()
    self.__idDict = {}
    for initFunc in ( self.__discoverSetup, self.__discoverTimeout, self.__discoverURL,
                      self.__discoverCredentialsToUse, self.__discoverExtraCredentials,
                      self.__checkTransportSanity ):
      result = initFunc()
      if not result[ 'OK' ]:
        self.__initStatus = result
        return
    #HACK for thread-safety:
    self.__allowedThreadID = False


  def __discoverSetup(self):
    #Which setup to use?
    if self.KW_SETUP in self.kwargs and self.kwargs[ self.KW_SETUP ]:
      self.setup = str( self.kwargs[ self.KW_SETUP ] )
    else:
      self.setup = gConfig.getValue( "/DIRAC/Setup", "LHCb-Development" )
    return S_OK()

  def __discoverURL(self):
    #Calculate final URL
    try:
      result = self.__findServiceURL()
    except Exception, e:
      return S_ERROR( str(e) )
    if not result[ 'OK' ]:
      return result
    self.serviceURL = result[ 'Value' ]
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
    if self.timeout:
      self.timeout = max( 600, self.timeout )
    self.kwargs[ self.KW_TIMEOUT ] = self.timeout
    return S_OK()

  def __discoverCredentialsToUse( self ):
    #Use certificates?
    if self.KW_USE_CERTIFICATES in self.kwargs:
      self.useCertificates = self.kwargs[ self.KW_USE_CERTIFICATES ]
    else:
      self.useCertificates = gConfig._useServerCertificate()
      self.kwargs[ self.KW_USE_CERTIFICATES ] = self.useCertificates
    if self.useCertificates:
      self.kwargs[ self.KW_SKIP_CA_CHECK ] = False
    else:
      self.kwargs[ self.KW_SKIP_CA_CHECK ] = CS.skipCACheck()
    if self.KW_PROXY_CHAIN in self.kwargs:
      try:
         self.kwargs[ self.KW_PROXY_STRING ] = self.kwargs[ self.KW_PROXY_CHAIN ].dumpAllToString()[ 'Value' ]
         del( self.kwargs[ self.KW_PROXY_CHAIN ] )
      except:
        return S_ERROR( "Invalid proxy chain specified on instantiation" )
    return S_OK()

  def __discoverExtraCredentials( self ):
    #Wich extra credentials to use?
    if self.useCertificates:
        self.__extraCredentials = self.VAL_EXTRA_CREDENTIALS_HOST
    else:
      self.__extraCredentials = ""
    if self.KW_EXTRA_CREDENTIALS in self.kwargs:
      self.__extraCredentials = self.kwargs[ self.KW_EXTRA_CREDENTIALS ]
    #Are we delegating something?
    if self.KW_DELEGATED_DN in self.kwargs:
      if self.KW_DELEGATED_GROUP in self.kwargs:
        self.__extraCredentials = self.kwargs[ self.KW_DELEGATED_GROUP ]
      self.__extraCredentials = ( self.kwargs[ self.KW_DELEGATED_DN ], self.__extraCredentials )
    return S_OK()

  def __findServiceURL( self ):
    for protocol in gProtocolDict.keys():
      if self.serviceName.find( "%s://" % protocol ) == 0:
        gLogger.debug( "Already given a valid url", self.serviceName )
        return S_OK( self.serviceName )

    if self.KW_IGNORE_GATEWAYS not in self.kwargs or not self.kwargs[ self.KW_IGNORE_GATEWAYS ]:
      dRetVal = gConfig.getOption( "/LocalSite/Site" )
      if dRetVal[ 'OK' ]:
        siteName = dRetVal[ 'Value' ]
        dRetVal = gConfig.getOption( "/DIRAC/Gateways/%s" % siteName )
        if dRetVal[ 'OK' ]:
          rawGatewayURL = List.randomize( List.fromChar( dRetVal[ 'Value'], "," ) )[0]
          gatewayURL = "/".join( rawGatewayURL.split( "/" )[:3] )
          gLogger.debug( "Using gateway", gatewayURL )
          return S_OK( "%s/%s" % (  gatewayURL, self.serviceName ) )

    try:
      urls = getServiceURL( self.serviceName, setup = self.setup )
    except Exception, e:
      return S_ERROR( "Cannot get URL for %s in setup %s: %s" % ( self.serviceName, self.setup, str(e) ) )
    if not urls:
      return S_ERROR( "URL for service %s not found" % self.serviceName )
    sURL = List.randomize( List.fromChar( urls, "," ) )[0]
    gLogger.debug( "Discovering URL for service", "%s -> %s" % ( self.serviceName, sURL ) )
    return S_OK( sURL )

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
    if not self.__initStatus[ 'OK' ]:
      return self.__initStatus
    self.__checkThreadID()
    gLogger.debug( "Connecting to: %s" % self.serviceURL )
    try:
      transport = gProtocolDict[ self.URLTuple[0] ][ 'transport' ]( self.URLTuple[1:3], **self.kwargs )
      retVal = transport.initAsClient()
      if not retVal[ 'OK' ]:
        return S_ERROR( "Can't connect to %s: %s" % ( self.serviceURL, retVal ) )
    except Exception, e:
      return S_ERROR( "Can't connect to %s: %s" % ( self.serviceURL, e ) )
    return S_OK( transport )

  def _proposeAction( self, transport, sAction ):
    stConnectionInfo = ( ( self.URLTuple[3], self.setup ), sAction, self.__extraCredentials )
    retVal = transport.sendData( S_OK( stConnectionInfo ) )
    if not retVal[ 'OK' ]:
        return retVal
    serverReturn = transport.receiveData()
    #TODO: Check if delegation is required
    if serverReturn[ 'OK' ] and 'Value' in serverReturn and type( serverReturn[ 'Value' ] ) == types.DictType:
      gLogger.debug( "There is a server requirement" )
      serverRequirements = serverReturn[ 'Value' ]
      if 'delegate' in serverRequirements:
        gLogger.debug( "A delegation is requested" )
        serverReturn = self.__delegateCredentials( transport, serverRequirements[ 'delegate' ] )
    return serverReturn

  def __delegateCredentials( self, transport, delegationRequest ):
    retVal = gProtocolDict[ self.URLTuple[0] ][ 'delegation' ]( delegationRequest, self.kwargs )
    if not retVal[ 'OK' ]:
      return retVal
    retVal = transport.sendData( retVal[ 'Value' ] )
    if not retVal[ 'OK' ]:
      return retVal
    return transport.receiveData()

  def __checkTransportSanity( self ):
    retVal = gProtocolDict[ self.URLTuple[0] ][ 'sanity' ]( self.URLTuple[1:3], self.kwargs )
    if not retVal[ 'OK' ]:
      return S_ERROR( "Insane environment for protocol: %s" % retVal[ 'Message' ] )
    idDict = retVal[ 'Value' ]
    for key in idDict:
      self.__idDict[ key ] = idDict[ key ]
    return S_OK()

  def _getBaseStub( self ):
    newKwargs = dict( self.kwargs )
    if 'group' in self.__idDict and not self.KW_DELEGATED_GROUP in newKwargs:
      newKwargs[ self.KW_DELEGATED_GROUP ] = self.__idDict[ 'group' ]
    if 'DN' in self.__idDict and not self.KW_DELEGATED_DN in newKwargs:
      newKwargs[ self.KW_DELEGATED_DN ] = self.__idDict[ 'DN' ]
    if 'useCertificates' in newKwargs:
      del( newKwargs[ 'useCertificates' ] )
    return ( self.serviceName, newKwargs )

  def __nonzero__( self ):
    return True

  def __str__( self ):
    return "<DISET Client %s %s>" % ( self.serviceURL, self.__extraCredentials )