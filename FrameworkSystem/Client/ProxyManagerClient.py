########################################################################
# $HeadURL$
########################################################################
""" ProxyManagementAPI has the functions to "talk" to the ProxyManagement service
"""
__RCSID__ = "$Id$"

import os
import datetime
import types
from DIRAC.Core.Utilities import Time, ThreadSafe, DIRACSingleton
from DIRAC.Core.Utilities.DictCache import DictCache
from DIRAC.Core.Security import Locations, CS, File, Properties
from DIRAC.Core.Security.X509Chain import X509Chain, g_X509ChainType
from DIRAC.Core.Security.X509Request import X509Request
from DIRAC.Core.Security.VOMS import VOMS
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC import S_OK, S_ERROR, gLogger

gUsersSync = ThreadSafe.Synchronizer()
gProxiesSync = ThreadSafe.Synchronizer()
gVOMSProxiesSync = ThreadSafe.Synchronizer()

class ProxyManagerClient:
  __metaclass__ = DIRACSingleton.DIRACSingleton

  def __init__( self ):
    self.__usersCache = DictCache()
    self.__proxiesCache = DictCache()
    self.__vomsProxiesCache = DictCache()
    self.__pilotProxiesCache = DictCache()
    self.__filesCache = DictCache( self.__deleteTemporalFile )

  def __deleteTemporalFile( self, filename ):
    try:
      os.unlink( filename )
    except:
      pass

  def clearCaches( self ):
    self.__usersCache.purgeAll()
    self.__proxiesCache.purgeAll()
    self.__vomsProxiesCache.purgeAll()
    self.__pilotProxiesCache.purgeAll()

  def __getSecondsLeftToExpiration( self, expiration, utc = True ):
    if utc:
      td = expiration - datetime.datetime.utcnow()
    else:
      td = expiration - datetime.datetime.now()
    return td.days * 86400 + td.seconds

  def __refreshUserCache( self, validSeconds = 0 ):
    rpcClient = RPCClient( "Framework/ProxyManager", timeout = 120 )
    retVal = rpcClient.getRegisteredUsers( validSeconds )
    if not retVal[ 'OK' ]:
      return retVal
    data = retVal[ 'Value' ]
    #Update the cache
    for record in data:
      cacheKey = ( record[ 'DN' ], record[ 'group' ] )
      self.__usersCache.add( cacheKey,
                             self.__getSecondsLeftToExpiration( record[ 'expirationtime' ] ),
                             record )
    return S_OK()

  @gUsersSync
  def userHasProxy( self, userDN, userGroup, validSeconds = 0 ):
    """
    Check if a user(DN-group) has a proxy in the proxy management
      - Updates internal cache if needed to minimize queries to the
          service
    """
    cacheKey = ( userDN, userGroup )
    if self.__usersCache.exists( cacheKey, validSeconds ):
      return S_OK( True )
    #Get list of users from the DB with proxys at least 300 seconds
    gLogger.verbose( "Updating list of users in proxy management" )
    retVal = self.__refreshUserCache( validSeconds )
    if not retVal[ 'OK' ]:
      return retVal
    return S_OK( self.__usersCache.exists( cacheKey, validSeconds ) )

  @gUsersSync
  def getUserPersistence( self, userDN, userGroup, validSeconds = 0 ):
    """
    Check if a user(DN-group) has a proxy in the proxy management
      - Updates internal cache if needed to minimize queries to the
          service
    """
    cacheKey = ( userDN, userGroup )
    userData = self.__usersCache.get( cacheKey, validSeconds )
    if userData:
      if userData[ 'persistent' ]:
        return S_OK( True )
    #Get list of users from the DB with proxys at least 300 seconds
    gLogger.verbose( "Updating list of users in proxy management" )
    retVal = self.__refreshUserCache( validSeconds )
    if not retVal[ 'OK' ]:
      return retVal
    userData = self.__usersCache.get( cacheKey, validSeconds )
    if userData:
      return S_OK( userData[ 'persistent' ] )
    return S_OK( False )

  def setPersistency( self, userDN, userGroup, persistent ):
    """
    Set the persistency for user/group
    """
    #Hack to ensure bool in the rpc call
    persistentFlag = True
    if not persistent:
      persistentFlag = False
    rpcClient = RPCClient( "Framework/ProxyManager", timeout = 120 )
    retVal = rpcClient.setPersistency( userDN, userGroup, persistentFlag )
    if not retVal[ 'OK' ]:
      return retVal
    #Update internal persistency cache
    cacheKey = ( userDN, userGroup )
    record = self.__usersCache.get( cacheKey, 0 )
    if record:
      record[ 'persistent' ] = persistentFlag
      self.__usersCache.add( cacheKey,
                             self.__getSecondsLeftToExpiration( record[ 'expirationtime' ] ),
                             record )
    return retVal

  def uploadProxy( self, proxy = False, diracGroup = False, chainToConnect = False, restrictLifeTime = 0 ):
    """
    Upload a proxy to the proxy management service using delegation
    """
    #Discover proxy location
    if type( proxy ) == g_X509ChainType:
      chain = proxy
      proxyLocation = ""
    else:
      if not proxy:
        proxyLocation = Locations.getProxyLocation()
        if not proxyLocation:
          return S_ERROR( "Can't find a valid proxy" )
      elif type( proxy ) in ( types.StringType, types.UnicodeType ):
        proxyLocation = proxy
      else:
        return S_ERROR( "Can't find a valid proxy" )
      chain = X509Chain()
      result = chain.loadProxyFromFile( proxyLocation )
      if not result[ 'OK' ]:
        return S_ERROR( "Can't load %s: %s " % ( proxyLocation, result[ 'Message' ] ) )

    if not chainToConnect:
      chainToConnect = chain

    #Make sure it's valid
    if chain.hasExpired()[ 'Value' ]:
      return S_ERROR( "Proxy %s has expired" % proxyLocation )

    #rpcClient = RPCClient( "Framework/ProxyManager", proxyChain = chainToConnect )
    rpcClient = RPCClient( "Framework/ProxyManager", timeout = 120 )
    #Get a delegation request
    result = rpcClient.requestDelegationUpload( chain.getRemainingSecs()['Value'], diracGroup )
    if not result[ 'OK' ]:
      return result
    #Check if the delegation has been granted
    if 'Value' not in result or not result[ 'Value' ]:
      if 'proxies' in result:
        return S_OK( result[ 'proxies' ] )
      else:
        return S_OK()
    reqDict = result[ 'Value' ]
    #Generate delegated chain
    chainLifeTime = chain.getRemainingSecs()[ 'Value' ] - 60
    if restrictLifeTime and restrictLifeTime < chainLifeTime:
      chainLifeTime = restrictLifeTime
    retVal = chain.generateChainFromRequestString( reqDict[ 'request' ],
                                                   lifetime = chainLifeTime,
                                                   diracGroup = diracGroup )
    if not retVal[ 'OK' ]:
      return retVal
    #Upload!
    result = rpcClient.completeDelegationUpload( reqDict[ 'id' ], retVal[ 'Value' ] )
    if not result[ 'OK' ]:
      return result
    if 'proxies' in result:
      return S_OK( result[ 'proxies' ] )
    return S_OK()


  @gProxiesSync
  def downloadProxy( self, userDN, userGroup, limited = False, requiredTimeLeft = 1200, 
                     cacheTime = 43200, proxyToConnect = False, token = False ):
    """
    Get a proxy Chain from the proxy management
    """
    cacheKey = ( userDN, userGroup )
    if self.__proxiesCache.exists( cacheKey, requiredTimeLeft ):
      return S_OK( self.__proxiesCache.get( cacheKey ) )
    req = X509Request()
    req.generateProxyRequest( limited = limited )
    if proxyToConnect:
      rpcClient = RPCClient( "Framework/ProxyManager", proxyChain = proxyToConnect, timeout = 120 )
    else:
      rpcClient = RPCClient( "Framework/ProxyManager", timeout = 120 )
    if token:
      retVal = rpcClient.getProxyWithToken( userDN, userGroup, req.dumpRequest()['Value'],
                                   long( cacheTime + requiredTimeLeft ), token )
    else:
      retVal = rpcClient.getProxy( userDN, userGroup, req.dumpRequest()['Value'],
                                   long( cacheTime + requiredTimeLeft ) )
    if not retVal[ 'OK' ]:
      return retVal
    chain = X509Chain( keyObj = req.getPKey() )
    retVal = chain.loadChainFromString( retVal[ 'Value' ] )
    if not retVal[ 'OK' ]:
      return retVal
    self.__proxiesCache.add( cacheKey, chain.getRemainingSecs()['Value'], chain )
    return S_OK( chain )

  def downloadProxyToFile( self, userDN, userGroup, limited = False, requiredTimeLeft = 1200, 
                           cacheTime = 43200, filePath = False, proxyToConnect = False, token = False ):
    """
    Get a proxy Chain from the proxy management and write it to file
    """
    retVal = self.downloadProxy( userDN, userGroup, limited, requiredTimeLeft, cacheTime, proxyToConnect, token )
    if not retVal[ 'OK' ]:
      return retVal
    chain = retVal[ 'Value' ]
    retVal = self.dumpProxyToFile( chain, filePath )
    if not retVal[ 'OK' ]:
      return retVal
    retVal[ 'chain' ] = chain
    return retVal

  @gVOMSProxiesSync
  def downloadVOMSProxy( self, userDN, userGroup, limited = False, requiredTimeLeft = 1200, 
                         cacheTime = 43200, requiredVOMSAttribute = False, proxyToConnect = False, token = False ):
    """
    Download a proxy if needed and transform it into a VOMS one
    """

    cacheKey = ( userDN, userGroup, requiredVOMSAttribute, limited )
    if self.__vomsProxiesCache.exists( cacheKey, requiredTimeLeft ):
      return S_OK( self.__vomsProxiesCache.get( cacheKey ) )
    req = X509Request()
    req.generateProxyRequest( limited = limited )
    if proxyToConnect:
      rpcClient = RPCClient( "Framework/ProxyManager", proxyChain = proxyToConnect, timeout = 120 )
    else:
      rpcClient = RPCClient( "Framework/ProxyManager", timeout = 120 )
    if token:
      retVal = rpcClient.getVOMSProxyWithToken( userDN, userGroup, req.dumpRequest()['Value'],
                                                long( cacheTime + requiredTimeLeft ), token, requiredVOMSAttribute )

    else:
      retVal = rpcClient.getVOMSProxy( userDN, userGroup, req.dumpRequest()['Value'],
                                       long( cacheTime + requiredTimeLeft ), requiredVOMSAttribute )
    if not retVal[ 'OK' ]:
      return retVal
    chain = X509Chain( keyObj = req.getPKey() )
    retVal = chain.loadChainFromString( retVal[ 'Value' ] )
    if not retVal[ 'OK' ]:
      return retVal
    self.__vomsProxiesCache.add( cacheKey, chain.getRemainingSecs()['Value'], chain )
    return S_OK( chain )

  def downloadVOMSProxyToFile( self, userDN, userGroup, limited = False, requiredTimeLeft = 1200, cacheTime = 43200, 
                               requiredVOMSAttribute = False, filePath = False, proxyToConnect = False, token = False ):
    """
    Download a proxy if needed, transform it into a VOMS one and write it to file
    """
    retVal = self.downloadVOMSProxy( userDN, userGroup, limited, requiredTimeLeft, cacheTime, 
                                     requiredVOMSAttribute, proxyToConnect, token )
    if not retVal[ 'OK' ]:
      return retVal
    chain = retVal[ 'Value' ]
    retVal = self.dumpProxyToFile( chain, filePath )
    if not retVal[ 'OK' ]:
      return retVal
    retVal[ 'chain' ] = chain
    return retVal

  def getPilotProxyFromDIRACGroup( self, userDN, userGroup, requiredTimeLeft = 43200, proxyToConnect = False ):
    """
    Download a pilot proxy with VOMS extensions depending on the group
    """
    #Assign VOMS attribute
    vomsAttr = CS.getVOMSAttributeForGroup( userGroup )
    if not vomsAttr:
      gLogger.verbose( "No voms attribute assigned to group %s when requested pilot proxy" % userGroup )
      return self.downloadProxy( userDN, userGroup, limited = False, requiredTimeLeft = requiredTimeLeft,
                                 proxyToConnect = proxyToConnect )
    else:
      return self.downloadVOMSProxy( userDN, userGroup, limited = False, requiredTimeLeft = requiredTimeLeft,
                                     requiredVOMSAttribute = vomsAttr, proxyToConnect = proxyToConnect )

  def getPilotProxyFromVOMSGroup( self, userDN, vomsAttr, requiredTimeLeft = 43200, proxyToConnect = False ):
    """
    Download a pilot proxy with VOMS extensions depending on the group
    """
    groups = CS.getGroupsWithVOMSAttribute( vomsAttr )
    if not groups:
      return S_ERROR( "No group found that has %s as voms attrs" % vomsAttr )

    for userGroup in groups:
      result = self.downloadVOMSProxy( userDN,
                                     userGroup,
                                     limited = False,
                                     requiredTimeLeft = requiredTimeLeft,
                                     requiredVOMSAttribute = vomsAttr,
                                     proxyToConnect = proxyToConnect )
      if result['OK']:
        return result
    return result

  def getPayloadProxyFromDIRACGroup( self, userDN, userGroup, requiredTimeLeft, token = False, proxyToConnect = False ):
    """
    Download a payload proxy with VOMS extensions depending on the group
    """
    #Assign VOMS attribute
    vomsAttr = CS.getVOMSAttributeForGroup( userGroup )
    if not vomsAttr:
      gLogger.verbose( "No voms attribute assigned to group %s when requested payload proxy" % userGroup )
      return self.downloadProxy( userDN, userGroup, limited = True, requiredTimeLeft = requiredTimeLeft,
                                 proxyToConnect = proxyToConnect, token = token )
    else:
      return self.downloadVOMSProxy( userDN, userGroup, limited = True, requiredTimeLeft = requiredTimeLeft,
                                     requiredVOMSAttribute = vomsAttr, proxyToConnect = proxyToConnect,
                                     token = token )

  def getPayloadProxyFromVOMSGroup( self, userDN, vomsAttr, token, requiredTimeLeft, proxyToConnect = False ):
    """
    Download a payload proxy with VOMS extensions depending on the VOMS attr
    """
    groups = CS.getGroupsWithVOMSAttribute( vomsAttr )
    if not groups:
      return S_ERROR( "No group found that has %s as voms attrs" % vomsAttr )
    userGroup = groups[0]

    return self.downloadVOMSProxy( userDN,
                                   userGroup,
                                   limited = True,
                                   requiredTimeLeft = requiredTimeLeft,
                                   requiredVOMSAttribute = vomsAttr,
                                   proxyToConnect = proxyToConnect,
                                   token = token )


  def dumpProxyToFile( self, chain, destinationFile = False, requiredTimeLeft = 600 ):
    """
    Dump a proxy to a file. It's cached so multiple calls won't generate extra files
    """
    result = chain.hash()
    if not result[ 'OK' ]:
      return result
    hash = result[ 'Value' ]
    if self.__filesCache.exists( hash, requiredTimeLeft ):
      filepath = self.__filesCache.get( hash )
      if os.path.isfile( filepath ):
        return S_OK( filepath )
      self.__filesCache.delete( hash )
    retVal = chain.dumpAllToFile( destinationFile )
    if not retVal[ 'OK' ]:
      return retVal
    filename = retVal[ 'Value' ]
    self.__filesCache.add( hash, chain.getRemainingSecs()['Value'], filename )
    return S_OK( filename )

  def deleteGeneratedProxyFile( self, chain ):
    """
    Delete a file generated by a dump
    """
    self.__filesCache.delete( chain )
    return S_OK()

  def requestToken( self, requesterDN, requesterGroup, numUses = 1 ):
    """
    Request a number of tokens. usesList must be a list of integers and each integer is the number of uses a token
    must have
    """
    rpcClient = RPCClient( "Framework/ProxyManager", timeout = 120 )
    return rpcClient.generateToken( requesterDN, requesterGroup, numUses )

  def renewProxy( self, proxyToBeRenewed = False, minLifeTime = 3600, newProxyLifeTime = 43200, proxyToConnect = False ):
    """
    Renew a proxy using the ProxyManager
    Arguments:
      proxyToBeRenewed : proxy to renew
      minLifeTime : if proxy life time is less than this, renew. Skip otherwise
      newProxyLifeTime : life time of new proxy
      proxyToConnect : proxy to use for connecting to the service
    """
    retVal = File.multiProxyArgument( proxyToBeRenewed )
    if not retVal[ 'Value' ]:
      return retVal
    proxyToRenewDict = retVal[ 'Value' ]

    secs = proxyToRenewDict[ 'chain' ].getRemainingSecs()[ 'Value' ]
    if secs > minLifeTime:
      File.deleteMultiProxy( proxyToRenewDict )
      return S_OK()

    if not proxyToConnect:
      proxyToConnectDict = { 'chain': False, 'tempFile': False }
    else:
      retVal = File.multiProxyArgument( proxyToConnect )
      if not retVal[ 'Value' ]:
        File.deleteMultiProxy( proxyToRenewDict )
        return retVal
      proxyToConnectDict = retVal[ 'Value' ]

    userDN = proxyToRenewDict[ 'chain' ].getIssuerCert()[ 'Value' ].getSubjectDN()[ 'Value' ]
    retVal = proxyToRenewDict[ 'chain' ].getDIRACGroup()
    if not retVal[ 'OK' ]:
      File.deleteMultiProxy( proxyToRenewDict )
      File.deleteMultiProxy( proxyToConnectDict )
      return retVal
    userGroup = retVal[ 'Value' ]
    limited = proxyToRenewDict[ 'chain' ].isLimitedProxy()[ 'Value' ]

    voms = VOMS()
    retVal = voms.getVOMSAttributes( proxyToRenewDict[ 'chain' ] )
    if not retVal[ 'OK' ]:
      File.deleteMultiProxy( proxyToRenewDict )
      File.deleteMultiProxy( proxyToConnectDict )
      return retVal
    vomsAttrs = retVal[ 'Value' ]
    if vomsAttrs:
      retVal = self.downloadVOMSProxy( userDN,
                                       userGroup,
                                       limited = limited,
                                       requiredTimeLeft = newProxyLifeTime,
                                       requiredVOMSAttribute = vomsAttrs[0],
                                       proxyToConnect = proxyToConnectDict[ 'chain' ] )
    else:
      retVal = self.downloadProxy( userDN,
                                   userGroup,
                                   limited = limited,
                                   requiredTimeLeft = newProxyLifeTime,
                                   proxyToConnect = proxyToConnectDict[ 'chain' ] )

    File.deleteMultiProxy( proxyToRenewDict )
    File.deleteMultiProxy( proxyToConnectDict )

    if not retVal[ 'OK' ]:
      return retVal

    chain = retVal['Value']

    if not proxyToRenewDict[ 'tempFile' ]:
      return chain.dumpAllToFile( proxyToRenewDict[ 'file' ] )

    return S_OK( chain )

  def getDBContents( self, condDict = {} ):
    """
    Get the contents of the db
    """
    rpcClient = RPCClient( "Framework/ProxyManager", timeout = 120 )
    return rpcClient.getContents( condDict, [ [ 'UserDN', 'DESC' ] ], 0, 0 )

  def getVOMSAttributes( self, chain ):
    """
    Get the voms attributes for a chain
    """
    return VOMS().getVOMSAttributes( chain )

  def getUploadedProxyLifeTime( self, DN, group ):
    """
    Get the remaining seconds for an uploaded proxy
    """
    result = self.getDBContents( { 'UserDN' : [ DN ], 'UserGroup' : [ group ] } )
    if not result[ 'OK' ]:
      return result
    data = result[ 'Value' ]
    if len( data[ 'Records' ] ) == 0:
      return S_OK( 0 )
    pNames = list( data[ 'ParameterNames' ] )
    dnPos = pNames.index( 'UserDN' )
    groupPos = pNames.index( 'UserGroup' )
    expiryPos = pNames.index( 'ExpirationTime' )
    for row in data[ 'Records' ]:
      if DN == row[ dnPos ] and group == row[ groupPos ]:
        td = row[ expiryPos ] - datetime.datetime.utcnow()
        secondsLeft = td.days * 86400 + td.seconds
        return S_OK( max( 0, secondsLeft ) )
    return S_OK( 0 )

  def getUserProxiesInfo( self ):
    """ Get the user proxies uploaded info
    """
    result = RPCClient( "Framework/ProxyManager", timeout = 120 ).getUserProxiesInfo()
    if 'rpcStub' in result:
      result.pop( 'rpcStub' )
    return result


gProxyManager = ProxyManagerClient()
