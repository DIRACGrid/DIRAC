# $HeadURL$
__RCSID__ = "$Id$"

import types
import thread
import DIRAC
from DIRAC.Core.DISET.private.Protocols import gProtocolDict
from DIRAC.FrameworkSystem.Client.Logger import gLogger
from DIRAC.Core.Utilities import List, Network
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Config import gConfig
from DIRAC.ConfigurationSystem.Client.PathFinder import getServiceURL
from DIRAC.Core.Security import CS
from DIRAC.Core.DISET.private.TransportPool import getGlobalTransportPool
from DIRAC.Core.DISET.ThreadConfig import ThreadConfig

class BaseClient:

  VAL_EXTRA_CREDENTIALS_HOST = "hosts"

  KW_USE_CERTIFICATES = "useCertificates"
  KW_EXTRA_CREDENTIALS = "extraCredentials"
  KW_TIMEOUT = "timeout"
  KW_SETUP = "setup"
  KW_VO = "VO"
  KW_DELEGATED_DN = "delegatedDN"
  KW_DELEGATED_GROUP = "delegatedGroup"
  KW_IGNORE_GATEWAYS = "ignoreGateways"
  KW_PROXY_LOCATION = "proxyLocation"
  KW_PROXY_STRING = "proxyString"
  KW_PROXY_CHAIN = "proxyChain"
  KW_SKIP_CA_CHECK = "skipCACheck"
  KW_KEEP_ALIVE_LAPSE = "keepAliveLapse"

  __threadConfig = ThreadConfig()

  def __init__( self, serviceName, **kwargs ):
    if type( serviceName ) not in types.StringTypes:
      raise TypeError( "Service name expected to be a string. Received %s type %s" %
                       ( str( serviceName ), type( serviceName ) ) )
    self._destinationSrv = serviceName
    self._serviceName = serviceName
    self.kwargs = kwargs
    self.__initStatus = S_OK()
    self.__idDict = {}
    self.__extraCredentials = ""
    self.__enableThreadCheck = False
    for initFunc in ( self.__discoverSetup, self.__discoverVO, self.__discoverTimeout,
                      self.__discoverURL, self.__discoverCredentialsToUse,
                      self.__checkTransportSanity,
                      self.__setKeepAliveLapse ):
      result = initFunc()
      if not result[ 'OK' ] and self.__initStatus[ 'OK' ]:
        self.__initStatus = result
    self._initialize()
    #HACK for thread-safety:
    self.__allowedThreadID = False


  def _initialize( self ):
    pass

  def getDestinationService( self ):
    return self._destinationSrv

  def getServiceName( self ):
    return self._serviceName

  def __discoverSetup( self ):
    #Which setup to use?
    if self.KW_SETUP in self.kwargs and self.kwargs[ self.KW_SETUP ]:
      self.setup = str( self.kwargs[ self.KW_SETUP ] )
    else:
      self.setup = self.__threadConfig.getSetup()
      if not self.setup:
        self.setup = gConfig.getValue( "/DIRAC/Setup", "Test" )
    return S_OK()

  def __discoverVO( self ):
    #Which setup to use?
    if self.KW_VO in self.kwargs and self.kwargs[ self.KW_VO ]:
      self.vo = str( self.kwargs[ self.KW_VO ] )
    else:
      self.vo = gConfig.getValue( "/DIRAC/VirtualOrganization", "unknown" )
    return S_OK()

  def __discoverURL( self ):
    #Calculate final URL
    try:
      result = self.__findServiceURL()
    except Exception, e:
      return S_ERROR( str( e ) )
    if not result[ 'OK' ]:
      return result
    self.serviceURL = result[ 'Value' ]
    retVal = Network.splitURL( self.serviceURL )
    if not retVal[ 'OK' ]:
      return S_ERROR( "URL is malformed: %s" % retVal[ 'Message' ] )
    self.__URLTuple = retVal[ 'Value' ]
    self._serviceName = self.__URLTuple[-1]
    return S_OK()

  def __discoverTimeout( self ):
    if self.KW_TIMEOUT in self.kwargs:
      self.timeout = self.kwargs[ self.KW_TIMEOUT ]
    else:
      self.timeout = False
    if self.timeout:
      self.timeout = max( 120, self.timeout )
    else:
      self.timeout = 600
    self.kwargs[ self.KW_TIMEOUT ] = self.timeout
    return S_OK()

  def __discoverCredentialsToUse( self ):
    #Use certificates?
    if self.KW_USE_CERTIFICATES in self.kwargs:
      self.useCertificates = self.kwargs[ self.KW_USE_CERTIFICATES ]
    else:
      self.useCertificates = gConfig._useServerCertificate()
      self.kwargs[ self.KW_USE_CERTIFICATES ] = self.useCertificates
    if self.KW_SKIP_CA_CHECK not in self.kwargs:
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
    delegatedDN, delegatedGroup = self.__threadConfig.getID()
    if self.KW_DELEGATED_DN in self.kwargs and self.kwargs[ self.KW_DELEGATED_DN ]:
      delegatedDN = self.kwargs[ self.KW_DELEGATED_DN ]
    elif delegatedDN:
      self.kwargs[ self.KW_DELEGATED_DN ] = delegatedDN
    if self.KW_DELEGATED_GROUP in self.kwargs and self.kwargs[ self.KW_DELEGATED_GROUP ]:
      delegatedGroup = self.kwargs[ self.KW_DELEGATED_GROUP ]
    elif delegatedGroup:
      self.kwargs[ self.KW_DELEGATED_GROUP ] = delegatedGroup
    if delegatedDN:
      if not delegatedGroup:
        result = CS.findDefaultGroupForDN( self.kwargs[ self.KW_DELEGATED_DN ] )
        if not result['OK']:
          return result
      self.__extraCredentials = ( delegatedDN, delegatedGroup )

    return S_OK()

  def __findServiceURL( self ):
    if not self.__initStatus[ 'OK' ]:
      return self.__initStatus
    gatewayURL = False
    if self.KW_IGNORE_GATEWAYS not in self.kwargs or not self.kwargs[ self.KW_IGNORE_GATEWAYS ]:
      dRetVal = gConfig.getOption( "/DIRAC/Gateways/%s" % DIRAC.siteName() )
      if dRetVal[ 'OK' ]:
        rawGatewayURL = List.randomize( List.fromChar( dRetVal[ 'Value'], "," ) )[0]
        gatewayURL = "/".join( rawGatewayURL.split( "/" )[:3] )

    for protocol in gProtocolDict.keys():
      if self._destinationSrv.find( "%s://" % protocol ) == 0:
        gLogger.debug( "Already given a valid url", self._destinationSrv )
        if not gatewayURL:
          return S_OK( self._destinationSrv )
        gLogger.debug( "Reconstructing given URL to pass through gateway" )
        path = "/".join( self._destinationSrv.split( "/" )[3:] )
        finalURL = "%s/%s" % ( gatewayURL, path )
        gLogger.debug( "Gateway URL conversion:\n %s -> %s" % ( self._destinationSrv, finalURL ) )
        return S_OK( finalURL )

    if gatewayURL:
      gLogger.debug( "Using gateway", gatewayURL )
      return S_OK( "%s/%s" % ( gatewayURL, self._destinationSrv ) )

    try:
      urls = getServiceURL( self._destinationSrv, setup = self.setup )
    except Exception, e:
      return S_ERROR( "Cannot get URL for %s in setup %s: %s" % ( self._destinationSrv, self.setup, str( e ) ) )
    if not urls:
      return S_ERROR( "URL for service %s not found" % self._destinationSrv )
    sURL = List.randomize( List.fromChar( urls, "," ) )[0]
    gLogger.debug( "Discovering URL for service", "%s -> %s" % ( self._destinationSrv, sURL ) )
    return S_OK( sURL )

  def __checkThreadID( self ):
    if not self.__initStatus[ 'OK' ]:
      return self.__initStatus
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
                                                                         cThID )
      gLogger.error( msgTxt )
      #raise Exception( msgTxt )


  def _connect( self ):
    deco = self.__threadConfig.getDecorator()
    if callable( deco ):
      return deco( self.__innerConnect )()
    return self.__innerConnect()

  def __innerConnect( self ):
    self.__discoverExtraCredentials()
    if not self.__initStatus[ 'OK' ]:
      return self.__initStatus
    if self.__enableThreadCheck:
      self.__checkThreadID()
    gLogger.debug( "Connecting to: %s" % self.serviceURL )
    try:
      transport = gProtocolDict[ self.__URLTuple[0] ][ 'transport' ]( self.__URLTuple[1:3], **self.kwargs )
      retVal = transport.initAsClient()
      if not retVal[ 'OK' ]:
        return S_ERROR( "Can't connect to %s: %s" % ( self.serviceURL, retVal ) )
    except Exception, e:
      return S_ERROR( "Can't connect to %s: %s" % ( self.serviceURL, e ) )
    trid = getGlobalTransportPool().add( transport )
    return S_OK( ( trid, transport ) )

  def _disconnect( self, trid ):
    getGlobalTransportPool().close( trid )

  def _proposeAction( self, transport, action ):
    if not self.__initStatus[ 'OK' ]:
      return self.__initStatus
    stConnectionInfo = ( ( self.__URLTuple[3], self.setup, self.vo ),
                         action,
                         self.__extraCredentials )
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
    retVal = gProtocolDict[ self.__URLTuple[0] ][ 'delegation' ]( delegationRequest, self.kwargs )
    if not retVal[ 'OK' ]:
      return retVal
    retVal = transport.sendData( retVal[ 'Value' ] )
    if not retVal[ 'OK' ]:
      return retVal
    return transport.receiveData()

  def __checkTransportSanity( self ):
    if not self.__initStatus[ 'OK' ]:
      return self.__initStatus
    retVal = gProtocolDict[ self.__URLTuple[0] ][ 'sanity' ]( self.__URLTuple[1:3], self.kwargs )
    if not retVal[ 'OK' ]:
      return S_ERROR( "Insane environment for protocol: %s" % retVal[ 'Message' ] )
    idDict = retVal[ 'Value' ]
    for key in idDict:
      self.__idDict[ key ] = idDict[ key ]
    return S_OK()

  def __setKeepAliveLapse( self ):
    kaa = 1
    if self.KW_KEEP_ALIVE_LAPSE in self.kwargs:
      try:
        kaa = max( 0, int( self.kwargs ) )
      except:
        pass
    if kaa:
      kaa = max( 150, kaa )
    self.kwargs[ self.KW_KEEP_ALIVE_LAPSE ] = kaa
    return S_OK()

  def _getBaseStub( self ):
    newKwargs = dict( self.kwargs )
    #Set DN
    tDN, tGroup = self.__threadConfig.getID()
    if not self.KW_DELEGATED_DN in newKwargs:
      if tDN:
        newKwargs[ self.KW_DELEGATED_DN ] = tDN
      elif 'DN' in self.__idDict:
        newKwargs[ self.KW_DELEGATED_DN ] = self.__idDict[ 'DN' ]
    #Discover group
    if not self.KW_DELEGATED_GROUP in newKwargs:
      if 'group' in self.__idDict:
        newKwargs[ self.KW_DELEGATED_GROUP ] = self.__idDict[ 'group' ]
      elif tGroup:
        newKwargs[ self.KW_DELEGATED_GROUP ] = tGroup
      else:
        if self.KW_DELEGATED_DN in newKwargs:
          if CS.getUsernameForDN( newKwargs[ self.KW_DELEGATED_DN ] )[ 'OK' ]:
            result = CS.findDefaultGroupForDN( newKwargs[ self.KW_DELEGATED_DN ] )
            if result['OK']:
              newKwargs[ self.KW_DELEGATED_GROUP ] = result['Value']
          if CS.getHostnameForDN( newKwargs[ self.KW_DELEGATED_DN ] )[ 'OK' ]:
            newKwargs[ self.KW_DELEGATED_GROUP ] = self.VAL_EXTRA_CREDENTIALS_HOST

    if 'useCertificates' in newKwargs:
      del( newKwargs[ 'useCertificates' ] )
    return ( self._destinationSrv, newKwargs )

  def __nonzero__( self ):
    return True

  def __str__( self ):
    return "<DISET Client %s %s>" % ( self.serviceURL, self.__extraCredentials )
